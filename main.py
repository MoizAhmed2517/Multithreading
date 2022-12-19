import time
from threading import Thread
from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
import sqlite3

conn = sqlite3.connect('test.db')

sql_create_table = ''' CREATE TABLE IF NOT EXISTS power_analyzer (
	id INTEGER NOT NULL UNIQUE,
	device_id INTEGER NOT NULL,
	voltage_a_n	INTEGER,
	voltage_b_n INTEGER,
	voltage_c_n INTEGER,
	voltage_l_n_avg INTEGER,
	voltage_a_b INTEGER,
	voltage_b_c INTEGER,
	voltage_c_a INTEGER,
	voltage_l_l_avg INTEGER,
	voltage_unbalance_a_n INTEGER,
	voltage_unbalance_b_n INTEGER,
	voltage_unbalance_c_n INTEGER,
	voltage_unbalance_l_n_avg INTEGER,
	voltage_unbalance_a_b INTEGER,
	voltage_unbalance_b_c INTEGER,
	voltage_unbalance_c_a INTEGER,
	voltage_unbalance_l_l_avg INTEGER,
	current_a INTEGER,
	current_b INTEGER,
	current_c INTEGER,
	current_avg INTEGER,
	current_n INTEGER,
	current_unbalance_a INTEGER,
	current_unbalance_b INTEGER,
	current_unbalance_c INTEGER,
	current_unbalance_avg INTEGER,
	power_factor_total INTEGER,
	power_factor_a INTEGER,
	power_factor_b INTEGER,
	power_factor_c INTEGER,
	displacement_power_factor_total INTEGER,
	displacement_power_factor_a INTEGER,
	displacement_power_factor_b INTEGER,
	displacement_power_factor_c INTEGER,
	frequency INTEGER,
	active_power_total INTEGER,
	active_power_a INTEGER,
	active_power_b INTEGER,
	active_power_c INTEGER,
	reactive_power_total INTEGER,
	reactive_power_a INTEGER,
	reactive_power_b INTEGER,
	reactive_power_c INTEGER,
	apparent_power_total INTEGER,
	apparent_power_a INTEGER,
	apparent_power_b INTEGER,
	apparent_power_c INTEGER,
	active_energy_delivered INTEGER,
	active_energy_received INTEGER,
	reactive_energy_delivered INTEGER,
	reactive_energy_received INTEGER,
	datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY(id AUTOINCREMENT)
);'''

sql_insert_data = '''INSERT INTO power_analyzer
    (device_id, voltage_a_n, voltage_b_n, voltage_c_n, voltage_l_n_avg, voltage_a_b, voltage_b_c, voltage_c_a, 
    voltage_l_l_avg, voltage_unbalance_a_n, voltage_unbalance_b_n, voltage_unbalance_c_n, voltage_unbalance_l_n_avg,
    voltage_unbalance_a_b, voltage_unbalance_b_c, voltage_unbalance_c_a, voltage_unbalance_l_l_avg, current_a, 
    current_b, current_c, current_avg, current_n, current_unbalance_a, current_unbalance_b, current_unbalance_c,
    current_unbalance_avg, power_factor_total, power_factor_a, power_factor_b, power_factor_c,
    displacement_power_factor_total, displacement_power_factor_a, displacement_power_factor_b, displacement_power_factor_c,
    frequency, active_power_total, active_power_a, active_power_b, active_power_c, reactive_power_total, reactive_power_a,
    reactive_power_b, reactive_power_c, apparent_power_total, apparent_power_a, apparent_power_b, apparent_power_c, 
    active_energy_delivered, active_energy_received, reactive_energy_delivered, reactive_energy_received) 
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
    '''

cur = conn.cursor()
cur.execute(sql_create_table)

global final_lst
final_lst = []
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
    # while True:
    measured_lst_1 = []
    measured_lst_2 = []
    try:
        t1 = time.time()
        result_first_half = host.read_holding_registers(256, count=50, unit=addr)
        result_second_half = host.read_holding_registers(306, count=50, unit=addr)
        for i in range(0, len(result_first_half.registers), 2):
            val_decoded = BinaryPayloadDecoder.fromRegisters(result_first_half.registers[i:i + 2],
                                                             byteorder=Endian.Big,
                                                             wordorder=Endian.Little)
            val_decoded2 = BinaryPayloadDecoder.fromRegisters(result_second_half.registers[i:i + 2],
                                                              byteorder=Endian.Big,
                                                              wordorder=Endian.Little)
            measured_val = round(val_decoded.decode_32bit_float(), 2)
            measured_val2 = round(val_decoded2.decode_32bit_float(), 2)
            measured_lst_1.append(measured_val)
            measured_lst_2.append(measured_val2)
    except:
        print(f"{my_client} not available")
    measured_lst = measured_lst_1 + measured_lst_2
    final_lst.append(measured_lst)


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
    thread = thread_list[i].join()

thread_list[-1].join()
print(final_lst)

for i in final_lst:
    device_id = 32
    i.insert(0, device_id)
    val = tuple(i)
    cur.execute(sql_insert_data, val)
    conn.commit()
    device_id = device_id + 32

conn.close()
