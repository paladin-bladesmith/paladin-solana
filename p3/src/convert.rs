use jito_protos::proto::{
    bundle::Bundle as ProtoBundle,
    bundle::BundleUuid as ProtoBundleUuid,
    packet::{
        Meta as ProtoMeta, Packet as ProtoPacket, PacketBatch as ProtoPacketBatch,
        PacketFlags as ProtoPacketFlags,
    },
    shared::Header,
};
use prost_types::Timestamp;
use solana_perf::packet::Packet;

pub fn packet_to_proto_packet(p: &Packet) -> Option<ProtoPacket> {
    Some(ProtoPacket {
        data: p.data(..)?.to_vec(),
        meta: Some(ProtoMeta {
            size: p.meta().size as u64,
            addr: p.meta().addr.to_string(),
            port: p.meta().port as u32,
            flags: Some(ProtoPacketFlags {
                discard: p.meta().discard(),
                forwarded: p.meta().forwarded(),
                repair: p.meta().repair(),
                simple_vote_tx: p.meta().is_simple_vote_tx(),
                tracer_packet: p.meta().is_perf_track_packet(),
                from_staked_node: p.meta().is_from_staked_node(),
            }),
            sender_stake: 0,
        }),
    })
}

pub fn proto_packets_to_batch(p: Vec<ProtoPacket>) -> Option<ProtoPacketBatch> {
    Some(ProtoPacketBatch { packets: p })
}

pub fn proto_packets_to_bundle(
    p: Vec<ProtoPacket>,
    ts: Timestamp,
    s: String,
) -> Option<ProtoBundleUuid> {
    Some(ProtoBundleUuid {
        bundle: Some(ProtoBundle {
            packets: p,
            header: Some(Header { ts: Some(ts) }),
        }),
        uuid: s,
    })
}
