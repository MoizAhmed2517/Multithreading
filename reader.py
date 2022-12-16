from threading import Thread
from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
import time

HOST = "10.1.249.2"
PORT = 502
SAMPLE_RATE = 5

# Mapping for MOXA-1
# unit_mod_add_mox_1 = {'PU Curring Oven': 50, 'Dotting department': 67, 'AHU Fan control': 71, 'Dotting dept.': 72,
#                       'Ground Floor office 1': 74, 'Ground Floor office 2': 75, 'Methanol Mixing': 76, 'Kitchen': 77,
#                       'First Floor office': 109, 'Packaging & QC office': 111}

unit_mod_add_mox_1 = {'PU Curring Oven': 50, 'Dotting department': 67, 'AHU Fan control': 71, 'Dotting dept.': 72}

# DPMC520 Modbus Map
DPMC520 = {'Voltage A N': 256, 'Voltage B N': 258, 'Voltage C N': 260, 'Voltage A B': 264, 'Voltage B C': 266,
           'Voltage C A': 268, 'Current A': 288, 'Current B': 290, 'Current C': 292, 'Current N': 296,
           'Power factor total': 306, 'Frequency': 322, 'Active power total': 324, 'Active power A': 326,
           'Active power B': 328, 'Active power C': 330, 'Reactive power total': 332, 'Reactive power A': 334,
           'Reactive power B': 336, 'Reactive power C': 338, 'Apparent power total': 340, 'Apparent power A': 342,
           'Apparent power B': 344, 'Apparent power C': 346, 'Active energy delivered': 348,
           'Active energy received': 350, 'Reactive energy delivered': 352, 'Reactive energy received': 354}

def connect2me(addr, meter_var, host):
    measured_lst = []
    try:
        for index in meter_var.values():
            val_to_b_decoded = host.read_holding_registers(address=index, count=2, unit=addr)
            val_decoded = BinaryPayloadDecoder.fromRegisters(val_to_b_decoded.registers, byteorder=Endian.Big,
                                                             wordorder=Endian.Little)
            measured_val = round(val_decoded.decode_32bit_float(), 2)
            measured_lst.append(measured_val)
    except:
        print(f"{my_client} not available")
    print(measured_lst)


t = time.time()
my_client = ModbusTcpClient(host=HOST, port=PORT)
my_client.connect()
MOXA_1 = list(unit_mod_add_mox_1.values())
thread_list = []

for i in range(len(MOXA_1)):
    t = Thread(target=connect2me, args=(MOXA_1[i], DPMC520, my_client))
    thread_list.append(t)

for i in range(len(thread_list)):
    thread_list[i].start

print(thread_list)

# t1 = Thread(target=connect2me, args=(MOXA_1[0], DPMC520, my_client))
# t3 = Thread(target=connect2me, args=(MOXA_1[2], DPMC520, my_client))
# t2 = Thread(target=connect2me, args=(MOXA_1[1], DPMC520, my_client))
# t4 = Thread(target=connect2me, args=(MOXA_1[3], DPMC520, my_client))
# t5 = Thread(target=connect2me, args=(MOXA_1[4], DPMC520, my_client))
# t6 = Thread(target=connect2me, args=(MOXA_1[5], DPMC520, my_client))
# t7 = Thread(target=connect2me, args=(MOXA_1[6], DPMC520, my_client))
# t8 = Thread(target=connect2me, args=(MOXA_1[7], DPMC520, my_client))
# t9 = Thread(target=connect2me, args=(MOXA_1[8], DPMC520, my_client))
# t10 = Thread(target=connect2me, args=(MOXA_1[9], DPMC520, my_client))

# t1.start()
# t2.start()
# t3.start()
# t4.start()
# t4.join()
# print(time.time() - t)
# t5.start()
# t6.start()
# t7.start()
# t8.start()
# t9.start()
# t10.start()

# t1.join()
# t2.join()
# t3.join()
# t4.join()
# t5.join()
# t6.join()
# t7.join()
# t8.join()
# t9.join()
# t10.join()

