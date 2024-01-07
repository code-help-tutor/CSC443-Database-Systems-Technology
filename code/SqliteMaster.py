WeChat: cstutorcs
QQ: 749389476
Email: tutorcs@163.com
from tools import parse_length_from_payload


class SqliteMaster:
    def __init__(self, payload, s_type):
        bytes_number_ls, p = parse_length_from_payload(payload, s_type)
        self.type = payload[p: p + bytes_number_ls[0]].decode("utf-8")
        p += bytes_number_ls[0]
        self.name = payload[p: p + bytes_number_ls[1]].decode("utf-8")
        p += bytes_number_ls[1]
        self.tbl_name = payload[p: p + bytes_number_ls[2]].decode("utf-8")
        p += bytes_number_ls[2]
        self.rootpage = int.from_bytes(payload[p: p + bytes_number_ls[3]], byteorder='big')
        p += bytes_number_ls[3]
        self.sql = payload[p:].decode("utf-8")

