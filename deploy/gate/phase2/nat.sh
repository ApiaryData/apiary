#!/bin/sh
# A home router: NAT outbound traffic and drop unsolicited inbound traffic.
#   nat.sh MASQ    endpoint-independent mapping (a typical home router)
#   nat.sh RANDOM  a new random source port per destination (a symmetric NAT,
#                  which direct paths cannot be punched through)
set -e
mode="${1:-MASQ}"
wan_net="${WAN_PREFIX:-10.20.0.}"
# Forwarding is switched on by the compose file (sysctls); /proc/sys is read-only here.
[ "$(cat /proc/sys/net/ipv4/ip_forward)" = 1 ] || { echo "ip_forward is off"; exit 1; }
wan_if=$(ip -o -4 addr show | awk -v p="$wan_net" '$4 ~ "^" p {print $2}' | head -n1)
[ -n "$wan_if" ] || { echo "no WAN interface found"; exit 1; }
# Docker gives a container its default route through the first network it joined,
# which for the lab router is the lab bridge, i.e. the host. Send everything out
# the WAN side instead, and refuse to forward private addresses onto it: the open
# network does not route a site's private subnets. (Without this the host quietly
# routes between the bridges and the "NAT" is bypassed.)
wan_gw=$(echo "$wan_net" | sed 's/\.$//').1
ip route replace default via "$wan_gw" dev "$wan_if"
iptables -A FORWARD -o "$wan_if" ! -d "${wan_net}0/24" -j DROP
# The open network is not next door: a round trip across it takes real time.
if tc qdisc add dev "$wan_if" root netem delay "${WAN_DELAY:-12ms}" 2>/dev/null; then
  echo "WAN latency ${WAN_DELAY:-12ms} each way out of this router"
else
  echo "(netem is not available: no WAN latency)"
fi
if [ "$mode" = RANDOM ]; then
  iptables -t nat -A POSTROUTING -o "$wan_if" -j MASQUERADE --random
else
  iptables -t nat -A POSTROUTING -o "$wan_if" -j MASQUERADE
fi
iptables -A FORWARD -i "$wan_if" -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
iptables -A FORWARD -i "$wan_if" -j DROP
echo "NAT ($mode) up on $wan_if"
exec sleep infinity
