#!/bin/bash
# Run the ONNX backend's test against a real ONNX Runtime, in a container.
#
# Installs ONNX Runtime and the onnx package with pip, builds a small linear model
# (2*a + 0.5*b + 1), and runs the plan crate's `onnx` test with the library and
# model located. Needs Docker; run from the repository root.
set -euo pipefail
repo="$(pwd)"
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "$(cygpath -w "$repo" 2>/dev/null || echo "$repo"):/work" \
  -v apiary-cargo-registry:/usr/local/cargo/registry \
  -v apiary-target:/target -e CARGO_TARGET_DIR=/target -w /work \
  rust:1.99-bookworm bash -c '
set -euo pipefail
apt-get update -qq >/dev/null
apt-get install -y -qq python3-pip >/dev/null
pip install --break-system-packages -q onnxruntime onnx numpy
python3 - <<PY
import numpy as np, onnx
from onnx import helper, TensorProto, numpy_helper
w = numpy_helper.from_array(np.array([[2.0], [0.5]], dtype=np.float32), "W")
b = numpy_helper.from_array(np.array([1.0], dtype=np.float32), "B")
graph = helper.make_graph(
    [helper.make_node("MatMul", ["X", "W"], ["M"]), helper.make_node("Add", ["M", "B"], ["Y"])],
    "linear",
    [helper.make_tensor_value_info("X", TensorProto.FLOAT, [None, 2])],
    [helper.make_tensor_value_info("Y", TensorProto.FLOAT, [None, 1])],
    [w, b],
)
model = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 13)])
model.ir_version = 8
onnx.save(model, "/tmp/linear.onnx")
PY
export APIARY_ONNX_MODEL=/tmp/linear.onnx
export ORT_DYLIB_PATH="$(python3 -c "import onnxruntime,glob,os;print(glob.glob(os.path.join(os.path.dirname(onnxruntime.__file__),\"capi\",\"libonnxruntime.so*\"))[0])")"
echo "runtime: $ORT_DYLIB_PATH"
cargo test -p apiary-plan --features onnx --test onnx -- --nocapture
'
