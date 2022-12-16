import time
from threading import Thread
from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian

HOST = "10.1.249.2"
PORT = 502
SAMPLE_RATE = 5

unit_mod_add_mox_1 = {'PU Curring Oven': 50, 'Dotting department': 67, 'AHU Fan control': 71, 'Dotting dept.': 72,
                      'Ground Floor office 1': 74, 'Ground Floor office 2': 75, 'Methanol Mixing': 76, 'Kitchen': 77,
                      'First Floor office': 109, 'Packaging & QC office': 111}

DPMC520 = {'Voltage A N': 256, 'Voltage B N': 258, 'Voltage C N': 260, 'Voltage A B': 264, 'Voltage B C': 266,
           'Voltage C A': 268, 'Current A': 288, 'Current B': 290, 'Current C': 292, 'Current N': 296,
           'Power factor total': 306, 'Frequency': 322, 'Active power total': 324, 'Active power A': 326,
           'Active power B': 328, 'Active power C': 330, 'Reactive power total': 332, 'Reactive power A': 334,
           'Reactive power B': 336, 'Reactive power C': 338, 'Apparent power total': 340, 'Apparent power A': 342,
           'Apparent power B': 344, 'Apparent power C': 346, 'Active energy delivered': 348,
           'Active energy received': 350, 'Reactive energy delivered': 352, 'Reactive energy received': 354}


def connect2me(addr, meter_var, host):
    while True:
        measured_lst = []
        try:

            result_adha = host.read_holding_registers(256, count=50, unit=addr)
            result_poora = host.read_holding_registers(300, count=50, unit=addr)
            for i in range(0, len(result_adha.registers), 2):
                val_decoded = BinaryPayloadDecoder.fromRegisters(result_adha.registers[i:i + 2], byteorder=Endian.Big,
                                                                 wordorder=Endian.Little)
                val_decoded2 = BinaryPayloadDecoder.fromRegisters(result_poora.registers[i:i + 2], byteorder=Endian.Big,
                                                                  wordorder=Endian.Little)
                measured_val = round(val_decoded.decode_32bit_float(), 2)
                measured_val2 = round(val_decoded2.decode_32bit_float(), 2)
                measured_lst.append(measured_val)
                measured_lst.append(measured_val2)
        except:
            print(f"{my_client} not available")
        t2 = time.time()
        print(measured_lst)
        print(t2 - t1)


my_client = ModbusTcpClient(host=HOST, port=PORT)
my_client.connect()
MOXA_1 = list(unit_mod_add_mox_1.values())
thread_list = []

t1 = time.time()
for i in range(len(MOXA_1)):
    t = Thread(target=connect2me, args=(MOXA_1[i], DPMC520, my_client))
    thread_list.append(t)

for i in range(len(thread_list)):
    t1 = time.time()
    thread_list[i].start()

thread_list[-1].join()
