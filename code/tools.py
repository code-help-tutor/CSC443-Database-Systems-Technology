WeChat: cstutorcs
QQ: 749389476
Email: tutorcs@163.com
def varint_to_int(bv):
    # covert varint byte array to int follow the description in https://www.sqlite.org/fileformat.html
    # Return the converted int value and the offset.
    ri, offset = 0, 0
    for i in range(0, len(bv)):
        if i == 8:
            break
        b_flag = bv[i] >> 7
        ri = (ri << 7) | (bv[i] & 0x7F)
        if b_flag == 0:
            return ri, i + 1
    offset += 8
    return ri | (bv[len(bv) - 1] << offset), 9


def parse_length_from_payload(payload, who):
    # parse page payload to get the content
    N = payload[0]  # elements number
    p = 1
    bytes_number_ls = []
    # I don't know why this situation happened. For SqliteMaster table tpye, the element number needed to -2 but For
    # Employee and SqliteMaster index type, just -1. which is very strange to me.
    if who == 'employee':
        N = N - 1
    elif who == 'table':
        N = N - 2
    elif who == 'index':
        N = N - 1
    for i in range(0, N):
        s, offset = varint_to_int(payload[p:p + 9])
        p += offset
        # 0 for 0 size, 1~4 for 1~4 size
        if s == 5:
            # for signed int
            s = 6
        elif s == 6 or s == 7:
            # for signed int and IEEE float
            s = 8
        elif s > 12 and s % 2 == 0:
            # for BLOB
            s = (s - 12) // 2
        elif s > 13 and s % 2 == 1:
            s = (s - 13) // 2
        bytes_number_ls.append(s)
    return bytes_number_ls, p
