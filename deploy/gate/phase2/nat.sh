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
if [ "$mode" = RANDOM ]; then
  iptables -t nat -A POSTROUTING -o "$wan_if" -j MASQUERADE --random
else
  iptables -t nat -A POSTROUTING -o "$wan_if" -j MASQUERADE
fi
iptables -A FORWARD -i "$wan_if" -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
iptables -A FORWARD -i "$wan_if" -j DROP
echo "NAT ($mode) up on $wan_if"
exec sleep infinity
