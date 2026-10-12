//! Writing a staged plan down, and reading it back on any Node.
//!
//! `datafusion-proto` serialises the standard operators and their expressions.
//! This codec adds Apiary's three leaves: a Frame scan (its spec), a stage input
//! (which stage, how many Patches), and a held leaf (just its key: the rows are
//! not in the plan). A [`StageRecord`] is one stage as written to the comb store;
//! a [`PlanRecord`] is the query's stages together.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::{
    AsExecutionPlan, PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use serde::{Deserialize, Serialize};

use crate::exchange::{schema_from_bytes, schema_to_bytes};
use crate::scan::{FrameScanExec, ScanSpec};
use crate::stages::{Handoff, HeldExec, Stage, StageInputExec, StagePlan};

#[derive(Serialize, Deserialize)]
enum Leaf {
    Scan(ScanSpec),
    Input {
        stage: usize,
        patches: usize,
        partitions: usize,
        whole: bool,
        schema: Vec<u8>,
    },
    Held {
        key: String,
        partitions: usize,
        schema: Vec<u8>,
    },
}

/// The codec for Apiary's plan leaves.
#[derive(Debug, Default)]
pub struct PatchCodec;

fn internal(context: &str, e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Internal(format!("{context}: {e}"))
}

impl PhysicalExtensionCodec for PatchCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _converter: &dyn PhysicalProtoConverterExtension,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let leaf: Leaf =
            serde_json::from_slice(buf).map_err(|e| internal("Unreadable plan leaf", e))?;
        Ok(match leaf {
            Leaf::Scan(spec) => Arc::new(FrameScanExec::from_spec(spec)?),
            Leaf::Input {
                stage,
                patches,
                partitions,
                whole,
                schema,
            } => Arc::new(StageInputExec::new(
                stage,
                schema_from_bytes(&schema)?,
                patches,
                partitions,
                whole,
                None,
            )),
            Leaf::Held {
                key,
                partitions,
                schema,
            } => Arc::new(HeldExec::decoded(
                key,
                schema_from_bytes(&schema)?,
                partitions,
            )),
        })
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _converter: &dyn PhysicalProtoConverterExtension,
    ) -> DfResult<()> {
        let leaf = if let Some(scan) = node.downcast_ref::<FrameScanExec>() {
            Leaf::Scan(scan.spec().clone())
        } else if let Some(input) = node.downcast_ref::<StageInputExec>() {
            Leaf::Input {
                stage: input.stage,
                patches: input.patches,
                partitions: input.partitions,
                whole: input.whole,
                schema: schema_to_bytes(&node.schema())?,
            }
        } else if let Some(held) = node.downcast_ref::<HeldExec>() {
            Leaf::Held {
                key: held.key.clone(),
                partitions: node.output_partitioning().partition_count(),
                schema: schema_to_bytes(&node.schema())?,
            }
        } else {
            return Err(DataFusionError::NotImplemented(format!(
                "{} cannot be written into a plan record",
                node.name()
            )));
        };
        serde_json::to_writer(buf, &leaf).map_err(|e| internal("Cannot write plan leaf", e))
    }
}

/// One stage as written to the comb store.
#[derive(Serialize, Deserialize)]
pub struct StageRecord {
    /// Its id.
    pub id: usize,
    /// How its Patches hand over output.
    pub handoff: HandoffRecord,
    /// Its boundary plan, as `datafusion-proto` bytes.
    pub plan: Vec<u8>,
    /// Whether it reads rows held in the planning Node's memory.
    pub pinned: bool,
}

/// [`Handoff`], as written down.
#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum HandoffRecord {
    /// See [`Handoff::Split`].
    Split(usize),
    /// See [`Handoff::Whole`].
    Whole,
    /// See [`Handoff::Result`].
    Result,
}

impl From<Handoff> for HandoffRecord {
    fn from(h: Handoff) -> Self {
        match h {
            Handoff::Split(n) => Self::Split(n),
            Handoff::Whole => Self::Whole,
            Handoff::Result => Self::Result,
        }
    }
}

impl From<HandoffRecord> for Handoff {
    fn from(h: HandoffRecord) -> Self {
        match h {
            HandoffRecord::Split(n) => Self::Split(n),
            HandoffRecord::Whole => Self::Whole,
            HandoffRecord::Result => Self::Result,
        }
    }
}

/// A query's stages as written to the comb store, under `plans/<query>`.
#[derive(Serialize, Deserialize)]
pub struct PlanRecord {
    /// The query's id.
    pub query: String,
    /// The Node that planned it, which holds any pinned inputs.
    pub planner: String,
    /// The stages, in the order they run.
    pub stages: Vec<StageRecord>,
}

/// Write `plan` as a record.
pub fn encode_plan(query: &str, planner: &str, plan: &StagePlan) -> DfResult<Vec<u8>> {
    let codec = PatchCodec;
    let stages = plan
        .stages
        .iter()
        .map(|stage| {
            let node =
                PhysicalPlanNode::try_from_physical_plan(Arc::clone(&stage.boundary), &codec)?;
            Ok(StageRecord {
                id: stage.id,
                handoff: stage.handoff.into(),
                plan: node.encode_to_vec(),
                pinned: stage.is_pinned(),
            })
        })
        .collect::<DfResult<Vec<_>>>()?;
    serde_json::to_vec(&PlanRecord {
        query: query.to_string(),
        planner: planner.to_string(),
        stages,
    })
    .map_err(|e| internal("Cannot write the plan record", e))
}

/// Read a plan record back. Held leaves are not in it; a Node that did not plan
/// the query cannot run the stages that read them.
pub fn decode_plan(bytes: &[u8], ctx: &TaskContext) -> DfResult<(PlanRecord, StagePlan)> {
    let record: PlanRecord =
        serde_json::from_slice(bytes).map_err(|e| internal("Unreadable plan record", e))?;
    let codec = PatchCodec;
    let stages = record
        .stages
        .iter()
        .map(|s| {
            let node = PhysicalPlanNode::decode(s.plan.as_slice())
                .map_err(|e| internal("Unreadable stage plan", e))?;
            Ok(Stage {
                id: s.id,
                boundary: node.try_into_physical_plan(ctx, &codec)?,
                handoff: s.handoff.into(),
            })
        })
        .collect::<DfResult<Vec<_>>>()?;
    Ok((
        record,
        StagePlan {
            stages,
            held: Default::default(),
        },
    ))
}
