WeChat: cstutorcs
QQ: 749389476
Email: tutorcs@163.com
import time

from Employee import Employee
from EmployeeIndex import EmployeeIndex
from SqliteMaster import SqliteMaster
from tools import varint_to_int


def help_search_row_id(page_pointer, q_row_id):
    global stat_table_leaf, stat_table_interior, number_of_records_per_page
    db_file.seek(page_size * (page_pointer - 1), 0)
    page = db_file.read(page_size)
    page_type = int.from_bytes(page[0:1], byteorder='big')
    number_of_cells = int.from_bytes(page[3:5], byteorder='big')
    if page_type == 5:
        # stat_table_interior += 1
        right_most_pointer = int.from_bytes(page[8:12], byteorder='big')
        cell_pointers_array = get_cell_pointers_array(page[12: 12 + 2 * number_of_cells])
        n_pointer = None
        row_id_flag = False
        for cp in cell_pointers_array:
            p = int.from_bytes(page[cp:cp + 4], byteorder='big')
            cp += 4
            row_id, offset = varint_to_int(page[cp: cp + 9])
            if row_id >= q_row_id:
                n_pointer = p
                row_id_flag = True
                break
        if not row_id_flag:
            n_pointer = right_most_pointer
        t_employee = help_search_row_id(n_pointer, q_row_id)
        if t_employee is not None:
            return t_employee
    cell_pointers_array = get_cell_pointers_array(page[8: 8 + 2 * number_of_cells])
    if page_type == 13:
        if number_of_records_per_page < len(cell_pointers_array):
            number_of_records_per_page = len(cell_pointers_array)
        stat_table_leaf += 1
        for cp in cell_pointers_array:
            payload_number_of_bytes, offset = varint_to_int(page[cp: cp + 9])
            cp += offset
            rowid, offset = varint_to_int(page[cp: cp + 9])
            if rowid != q_row_id:
                continue
            cp += offset
            payload = page[cp: cp + payload_number_of_bytes]
            employee = Employee(payload)
            # print('Use uncluster index->Find at Table Leaf page', page_pointer, '->', employee)
            return employee


def get_cell_pointers_array(bstr):
    p_array = []
    # print(bstr)
    for i in range(0, len(bstr) // 2):
        p_array.append(int.from_bytes(bstr[i*2: i*2 + 2], byteorder='big'))
    return p_array


def search_page_from_page_pointer(page_pointer):
    global sqlite_master_index, sqlite_master_table, stat_index_leaf, stat_index_interior, stat_table_interior, \
        stat_table_leaf, number_of_records_per_page, number_of_index_per_page
    db_file.seek(page_size * (page_pointer - 1), 0)
    page = db_file.read(page_size)
    if page_pointer == 1:
        page = page[100:]
    page_type = int.from_bytes(page[0:1], byteorder='big')
    number_of_cells = int.from_bytes(page[3:5], byteorder='big')
    if page_type == 2 or page_type == 5:
        # This is a Interior page
        right_most_pointer = int.from_bytes(page[8:12], byteorder='big')
        cell_pointers_array = get_cell_pointers_array(page[12: 12 + 2 * number_of_cells])
        if page_type == 5:
            stat_table_interior += 1
            # This page is Table Interior.
            pointers = []
            for cp in cell_pointers_array:
                pointers.append(int.from_bytes(page[cp:cp+4], byteorder='big'))
            pointers.append(right_most_pointer)
            for pi in pointers:
                if search_page_from_page_pointer(pi):
                    return True
        elif page_type == 2:
            stat_index_interior += 1
            # This page is Index Interior.
            pointers = []
            equal_flag, range_flag = False, False
            last_employee = None
            if number_of_index_per_page < len(cell_pointers_array):
                number_of_index_per_page = len(cell_pointers_array)
            for cp in cell_pointers_array:
                p = int.from_bytes(page[cp:cp + 4], byteorder='big')
                cp += 4
                payload_number_of_bytes, offset = varint_to_int(page[cp: cp + 9])
                cp += offset
                payload = page[cp: cp + payload_number_of_bytes]
                # print(payload)
                employee_idx, employee = None, None
                if sqlite_master_index is not None:
                    employee_idx = EmployeeIndex(payload)
                else:
                    employee = Employee(payload)
                if q_method == 'scan':
                    pointers.append(p)
                    if sqlite_master_index is not None:
                        employee = help_search_row_id(sqlite_master_table.rootpage, employee_idx.row_id)
                        # print('test--->>', employee_idx, employee)
                        if q_col_value == employee.get_value(q_col_name):
                            print('Use unclusterd index', employee_idx, '-> Find at Index Interior page', page_pointer,
                                  '->', employee)
                    else:
                        if q_col_value == employee.get_value(q_col_name):
                            print('Find at Index Interior page', page_pointer, '->', employee)
                elif q_method == 'equal':
                    if sqlite_master_index is not None:
                        if q_col_value == employee_idx.emp_id:
                            employee = help_search_row_id(sqlite_master_table.rootpage, employee_idx.row_id)
                            print('Use unclusterd index', employee_idx, '-> Find at Index Interior page', page_pointer,
                                  '->', employee)
                            return True
                        if q_col_value < employee_idx.emp_id:
                            pointers.append(p)
                            equal_flag = True
                            break
                    else:
                        if q_col_value == employee.get_value(q_col_name):
                            print('Find at Index Interior page', page_pointer, '->', employee)
                            return True
                        if q_col_value < employee.get_value(q_col_name):
                            pointers.append(p)
                            equal_flag = True
                            break
                elif q_method == 'range':
                    if sqlite_master_index is not None:
                        last_employee = employee_idx
                        if q_col_value[0] < employee_idx.emp_id < q_col_value[1]:
                            pointers.append(p)
                        elif q_col_value[0] < employee_idx.emp_id and q_col_value[1] < employee_idx.emp_id:
                            pointers.append(p)
                            range_flag = True
                            break
                        if q_col_value[0] <= employee_idx.emp_id <= employee_idx.emp_id:
                            employee = help_search_row_id(sqlite_master_table.rootpage, employee_idx.row_id)
                            print('Use unclusterd index', employee_idx, '-> Find at Index Interior page',
                                  page_pointer, '->', employee)
                    else:
                        last_employee = employee
                        if q_col_value[0] < employee.get_value(q_col_name) < q_col_value[1]:
                            pointers.append(p)
                        elif q_col_value[0] < employee.get_value(q_col_name) \
                                and q_col_value[1] < employee.get_value(q_col_name):
                            pointers.append(p)
                            range_flag = True
                            break
                        if q_col_value[0] <= employee.get_value(q_col_name) <= q_col_value[1]:
                            print('Find at Index Interior page', page_pointer, '->', employee)
            if q_method == 'scan':
                pointers.append(right_most_pointer)
            if q_method == 'equal' and not equal_flag:
                pointers.append(right_most_pointer)
            if q_method == 'range' and not range_flag and q_col_value[1] > last_employee.get_value(q_col_name):
                pointers.append(right_most_pointer)
            for pi in pointers:
                if search_page_from_page_pointer(pi):
                    return True
    cell_pointers_array = get_cell_pointers_array(page[8: 8 + 2 * number_of_cells])
    if page_type == 13:
        if len(cell_pointers_array) != 1 and len(cell_pointers_array) != 2:
            stat_table_leaf += 1
            if number_of_records_per_page < len(cell_pointers_array):
                number_of_records_per_page = len(cell_pointers_array)
        if page_pointer == 1:
            for i in range(0, len(cell_pointers_array)):
                cell_pointers_array[i] -= 100
        cp_count = 0
        for cp in cell_pointers_array:
            cp_count += 1
            payload_number_of_bytes, offset = varint_to_int(page[cp: cp + 9])
            cp += offset
            rowid, offset = varint_to_int(page[cp: cp + 9])
            cp += offset
            payload = page[cp: cp + payload_number_of_bytes]
            # print(payload)
            if page_pointer == 1:
                if cp_count == 1:
                    sqlite_master_table = SqliteMaster(payload, 'table')
                else:
                    sqlite_master_index = SqliteMaster(payload, 'index')
            else:
                employee = Employee(payload)
                if q_method == 'scan' and q_col_value == employee.get_value(q_col_name):
                    print('Find at Table Leaf page', page_pointer, '->', employee)
                elif q_method == 'equal':
                    if q_col_value == employee.get_value(q_col_name):
                        print('Find at Table Leaf page', page_pointer, '->', employee)
                        return True
                elif q_method == 'range' and q_col_value[0] <= employee.get_value(q_col_name) <= q_col_value[1]:
                    print('Find at Table Leaf page', page_pointer, '->', employee)
    elif page_type == 10:
        stat_index_leaf += 1
        for cp in cell_pointers_array:
            payload_number_of_bytes, offset = varint_to_int(page[cp: cp + 9])
            cp += offset
            payload = page[cp: cp + payload_number_of_bytes]
            employee = None
            if sqlite_master_index is not None:
                employee_idx = EmployeeIndex(payload)
                # magic number and I still don't know why row_id=0 shows and it's actually number is 0.
                if employee_idx.row_id == 0:
                    employee_idx.row_id += 1
                if q_method == 'equal':
                    if q_col_value == employee_idx.emp_id:
                        employee = help_search_row_id(sqlite_master_table.rootpage, employee_idx.row_id)
                        print('Find at Index Leaf page', page_pointer, '->', employee)
                        return True
                    else:
                        continue
                if q_method == 'range':
                    if q_col_value[0] <= employee_idx.emp_id <= q_col_value[1]:
                        employee = help_search_row_id(sqlite_master_table.rootpage, employee_idx.row_id)
                        print('Find at Index Leaf page', page_pointer, '->', employee)
                        continue
                    else:
                        continue
                employee = help_search_row_id(sqlite_master_table.rootpage, employee_idx.row_id)
            else:
                employee = Employee(payload)
            if q_method == 'scan' and q_col_value == employee.get_value(q_col_name):
                print('Find at Index Leaf page', page_pointer, '->', employee)
            elif q_method == 'equal' and q_col_value == employee.get_value(q_col_name):
                print('Find at Index Leaf page', page_pointer, '->', employee)
                return True
            elif q_method == 'range' and q_col_value[0] <= employee.get_value(q_col_name) <= q_col_value[1]:
                print('Find at Index Leaf page', page_pointer, '->', employee)
    return False


stat_table_leaf, stat_table_interior, stat_index_leaf, stat_index_interior = 0, 0, 0, 0
q_method, q_col_name, q_col_value = 'scan', 'Last Name', 'Rowe'
db_file = None
sqlite_master_table, sqlite_master_index = None, None
page_size = 0
number_of_records_per_page, number_of_index_per_page = -1, -1


def query(db_file_name, method, col_name, col_value):
    global db_file, stat_table_interior, stat_table_leaf, stat_index_leaf, stat_index_interior, sqlite_master_table, \
        sqlite_master_index, q_method, q_col_value, q_col_name, page_size, number_of_records_per_page\
        , number_of_index_per_page
    q_method, q_col_name, q_col_value = method, col_name, col_value
    stat_table_leaf, stat_table_interior, stat_index_leaf, stat_index_interior = 0, 0, 0, 0
    number_of_records_per_page, number_of_index_per_page = -1, -1
    start_time = time.time()
    db_file = open(db_file_name, 'rb')
    db_file.seek(16, 0)
    page_size = int.from_bytes(db_file.read(2), byteorder='big')
    print('File:"{}" has page size of {}Bytes'.format(db_file_name, page_size))
    sqlite_master_table, sqlite_master_index = None, None
    search_page_from_page_pointer(1)  # set sqlite_master_table or sqlite_master_index
    if sqlite_master_index is not None:
        search_page_from_page_pointer(sqlite_master_index.rootpage)
    else:
        search_page_from_page_pointer(sqlite_master_table.rootpage)
    print('Read Table Interior Pages: {}\t\tRead Table Leaf Pages: {}'.format(stat_table_interior, stat_table_leaf))
    print('Read Index Interior Pages: {}\t\tRead Index Leaf Pages: {}'.format(stat_index_interior, stat_index_leaf))
    print('Number of records per data page: {}.\t\tNumber of index per page: {}.(-1 represents not have this value).'
          .format(number_of_records_per_page, number_of_index_per_page))
    total_pages = stat_index_interior + stat_index_leaf + stat_table_leaf + stat_table_interior
    average_time = (time.time() - start_time) / total_pages * 1000
    print('Total Page Read: {}\t\tAverage Read Page Time: {}ms'.format(total_pages, average_time))
