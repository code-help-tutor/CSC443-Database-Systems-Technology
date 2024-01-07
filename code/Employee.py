WeChat: cstutorcs
QQ: 749389476
Email: tutorcs@163.com
from tools import varint_to_int, parse_length_from_payload

cols_name_ls = ['Emp ID', 'Name Prefix', 'First Name', 'Middle Initial', 'Last Name', 'Gender', 'E Mail',
                "Father's Name", "Mother's Name", "Mother's Maiden Name", 'Date of Birth', 'Time of Birth',
                'Age in Yrs.', 'Weight in Kgs.', 'Date of Joining', 'Quarter of Joining', 'Half of Joining',
                'Year of Joining', 'Month of Joining', 'Month Name of Joining', 'Short Month', 'Day of Joining',
                'DOW of Joining', 'Short DOW', 'Age in Company (Years)', 'Salary', 'Last % Hike', 'SSN', 'Phone No. ',
                'Place Name', 'County', 'City', 'State', 'Zip', 'Region', 'User Name', 'Password']

cols_name_map = {}
for k in range(0, len(cols_name_ls)):
    cols_name_map[cols_name_ls[k]] = k


class Employee:
    def __init__(self, payload):
        bytes_number_ls, p = parse_length_from_payload(payload, 'employee')
        self.data = []
        for i in range(0, len(bytes_number_ls)):
            if i == 0:
                self.data.append(int.from_bytes(payload[p:p+bytes_number_ls[i]], byteorder='big'))
            else:
                self.data.append(payload[p:p+bytes_number_ls[i]])
            p += bytes_number_ls[i]

    def get_value(self, col_name):
        if col_name == 'Emp ID':
            return self.data[cols_name_map[col_name]]
        else:
            return self.data[cols_name_map[col_name]].decode("utf-8")

    def get_emp_id(self):
        return self.data[0]

    def get_fullname(self):
        return '{} {}'.format(self.data[2].decode("utf-8"), self.data[4].decode("utf-8"))

    def __str__(self):
        return 'EmpId: {}--FullName: {}'.format(self.data[0], self.get_fullname())

