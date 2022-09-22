from pysnmp import hlapi

hostip = '10.161.56.38'

# MAC-Adresstabelle dot1dTpFdbAddress (.1.3.6.1.2.1.17.4.3.1.1).
# Bridge-Portnummer dot1dTpFdbPort (.1.3.6.1.2.1.17.4.3.1.2).
# Bridge-Port auf ifIndex-Zuordnung, dot1dBasePortIfIndex (.1.3.6.1.2.1.17.1.4.1.2).
# ifName (.1.3.6.1.2.1.31.1.1.1).

from pysnmp.hlapi import *

vtpVlanState = '1.3.6.1.4.1.9.9.46.1.3.1.1.2'
dot1dTpFdbAddress = '1.3.6.1.2.1.17.4.3.1.1'
dot1dTpFdbPort = '1.3.6.1.2.1.17.4.3.1.2'
dot1dBasePortIfIndex = '1.3.6.1.2.1.17.1.4.1.2'
ifName = '1.3.6.1.2.1.31.1.1.1.1'
ifIndex = '1.3.6.1.2.1.2.2.1.1'

def get_snmp(oid):
    iterator = nextCmd(
        SnmpEngine(),
        UsmUserData('admin', 'hammer23'),
        UdpTransportTarget((hostip, 161)),
        ContextData(),
        ObjectType(ObjectIdentity(oid)),
        lexicographicMode=False
    )

    snmp_val = {}
    for errorIndication, errorStatus, errorIndex, varBinds in iterator:
        if errorIndication:
            print(errorIndication)
            break

        elif errorStatus:
            print('%s at %s' % (errorStatus.prettyPrint(),
                            errorIndex and varBinds[int(errorIndex) - 1][0] or '?'))
            break

        else:
            for varBind in varBinds:
                oid_num, oid_val = varBind
                snmp_val.update({f'{oid_num}'[len(oid):]: f'{oid_val.prettyPrint()}'})
    return snmp_val

FdbAddress = get_snmp(dot1dTpFdbAddress)
FdbPort = get_snmp(dot1dTpFdbPort)
mac_table = {}
for oid in FdbAddress.keys():
    hex_mac = FdbAddress[oid]
    mac = ':'.join([hex_mac[i:i+2] for i,j in enumerate(hex_mac) if not (i%2)])
    port = FdbPort[oid]
    mac_table.update({f'{mac[len("0x:"):]}': f'{port}'})
BasePortIfIndex = get_snmp(dot1dBasePortIfIndex)
IfName = get_snmp(ifName)
for ifidx in IfName.keys():
    name = IfName[ifidx]
    portnums = []
    for port in BasePortIfIndex.keys():
        if BasePortIfIndex[port] == ifidx.lstrip('.'):
            for mac in mac_table.keys():
                macidx = mac_table[mac]
                if macidx == port.lstrip('.'):
                    portnums.append(mac)
    print(f'{name}: {portnums}')
#IfIndex = get_snmp(ifIndex)
# print(f'{IfIndex}')
