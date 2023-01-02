def checkIfStringIsFloat(string):
    stringWithoutDot = string.replace(".", "", 1)
    stringWithoutMinus = stringWithoutDot.replace("-", "", 1)
    isStringDigit = stringWithoutMinus.isdigit()
    return isStringDigit
