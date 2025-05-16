import base64


# enrypting the given string
def enrypt_str(request,data):
    data = bytes(data, 'utf-8')
    data = base64.b64encode(data)
    data = data.decode()
    return data

# decrypting the given string
def decrypt_str(request,data,convert_to):
    data = bytes(data, 'utf-8')
    data = base64.b64decode(data)
    data = data.decode()
    if convert_to == 'int':
        data = int(data)
    elif convert_to == 'float':
        data = float(data)
    elif convert_to == 'str':
        data = str(data)
    return data
