def removeNonAscii(s):
    try:
        return "".join(filter(lambda x: ord(x)<128, s))
    except:
        temp = s
        result = ''
        for i in range(len(temp)):
            if i != 0:
                result += " " + "".join(filter(lambda x: ord(x)<128, temp[i]))
            else:
                result += "".join(filter(lambda x: ord(x)<128, temp[i]))
        return result