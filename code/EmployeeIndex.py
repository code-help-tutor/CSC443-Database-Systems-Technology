WeChat: cstutorcs
QQ: 749389476
Email: tutorcs@163.com
from tools import parse_length_from_payload


class EmployeeIndex:
    def __init__(self, payload):
        bytes_number_ls, p = parse_length_from_payload(payload, 'employee')
        self.emp_id = int.from_bytes(payload[p: p + bytes_number_ls[0]], byteorder='big')
        p += bytes_number_ls[0]
        self.row_id = int.from_bytes(payload[p: p + bytes_number_ls[1]], byteorder='big')

    def __str__(self):
        return 'emp_id:{} row_id:{}'.format(self.emp_id, self.row_id)

    def get_value(self, col_name):
        return self.emp_id
