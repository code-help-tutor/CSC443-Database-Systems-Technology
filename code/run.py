WeChat: cstutorcs
QQ: 749389476
Email: tutorcs@163.com
# When you generate file, you must place your db_file at the same dir with run.py and replace below file name
from query_db import query

if __name__ == '__main__':
    # db_file_ls = ['no_index_4kb.db', 'no_index_16kb.db', 'clustered_index_4kb.db', 'unclustered_index_4kb.db']
    db_file_ls = ['no_index_16kb.db']
    test_case = [
        ['equal', 'Emp ID', int('181162')],
        ['range', 'Emp ID', [int('171800'), int('171899')]],
        ['scan', 'Last Name', 'Rowe']
    ]
    test_case_situation = [
        'Equality search    --Query and print the full name of employee #181162',
        'Range search       -- Query and print the employee id and full name of all employees with ”Emp ID” between '
        '#171800 and #171899 ',
        'Scan Operation     --Query and print the employee id and full name of anybody whose last name is ”Rowe”'
    ]
    for db_file_name in db_file_ls:
        print('====================  Start Test "{}"==========================='.format(db_file_name))
        for i in range(0, len(test_case_situation)):
            case = test_case[i]
            print('******* Test Case:', test_case_situation[i])
            query(db_file_name, case[0], case[1], case[2])
        print('====================  End Test "{}"  ==========================='.format(db_file_name))
        print()
