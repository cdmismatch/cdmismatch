
import json

class AbstractIX:
    def __init__(self, city, country, id, media, name, name_long, policy_email, policy_phone, proto_ipv6, proto_multicast, proto_unicast, region_continent, tech_email, tech_phone, url_stats, website, **kwargs):
        self.city = city
        self.country = country
        self.id = id
        self.media = media
        self.name = name
        self.name_long = name_long
        self.policy_email = policy_email
        self.policy_phone = policy_phone
        self.proto_ipv6 = proto_ipv6
        self.proto_multicast = proto_multicast
        self.proto_unicast = proto_unicast
        self.region_continent = region_continent
        self.tech_email = tech_email
        self.tech_phone = tech_phone
        self.url_stats = url_stats
        self.website = website
        self.kwargs = kwargs

    def __repr__(self):
        return '<IX {}>'.format(self.name)

class IX(AbstractIX):
    def __init__(self, created, notes, org_id, status, updated, **kwargs):
        super().__init__(**kwargs)
        self.created = created
        self.notes = notes
        self.org_id = org_id
        self.status = status
        self.updated = updated

    def __repr__(self):
        return '<IX {}>'.format(self.name)


class IXLAN:
    def __init__(self, ix, arp_sponge, created, descr, dot1q_support, id, ix_id, mtu, name, rs_asn, status, updated, **kwargs):
        self.ix = ix
        self.arp_sponge = arp_sponge
        self.created = created
        self.descr = descr
        self.dot1q_support = dot1q_support
        self.id = id
        self.ix_id = ix_id
        self.mtu = mtu
        self.name = name
        self.rs_asn = rs_asn
        self.status = status
        self.updated = updated


class IXPFX:
    def __init__(self, ixlan, created, id, ixlan_id, prefix, protocol, status, updated, **kwargs):
        self.ixlan = ixlan
        self.created = created
        self.id = id
        self.ixlan_id = ixlan_id
        self.prefix = prefix
        self.protocol = protocol
        self.status = status
        self.updated = updated

    def __repr__(self):
        return '<IXPFX Name={}, Prefix={}>'.format(self.ixlan.ix.name, self.prefix)


class NetIXLAN:
    def __init__(self, ixlan, asn, created, id, ipaddr4, ipaddr6, is_rs_peer, ixlan_id, net_id, notes, speed,
                 status, updated, name=None, **kwargs):
        self.ix = ixlan.ix
        self.ixlan = ixlan
        self.asn = asn
        self.created = created
        self.id = id
        self.ipaddr4 = ipaddr4
        self.ipaddr6 = ipaddr6
        self.is_rs_peer = is_rs_peer
        self.ix_id = self.ix.id
        self.ixlan_id = ixlan_id
        self.name = name
        self.net_id = net_id
        self.notes = notes
        self.speed = speed
        self.status = status
        self.updated = updated

    def __repr__(self):
        return '<NetIXLAN ASN={}, IX={}>'.format(self.asn, self.ix.name)



if __name__ == '__main__':
    pass
