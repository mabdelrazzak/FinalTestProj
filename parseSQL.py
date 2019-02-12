def sqlNULLParser(sqlStatement):
    sqlStatement = sqlStatement.upper()

    posS = sqlStatement.find("SELECT")
    posF = sqlStatement.find("FROM")
    start = posS + len("SELECT ")

    oldListOfFields = sqlStatement[start:posF]
    listOfFields = oldListOfFields.replace(" ", "").split(",")
    #
    # ListOfFieldsNVL = []
    # for field in listOfFields:
    #     newField = "coalesce(" + field + ",'')" + " as " + field
    #     ListOfFieldsNVL.append(newField)

    ListOfFieldsNVL = []
    for field in listOfFields:
        fieldName = field.split(
            ".")  # If the field name has its table name as part of its name then split it to use the field name only as alias (ex: PAYMENTS.ORDER_ID, we need ORDER_ID to be the alias)
        if len(fieldName) > 1:
            fieldName = fieldName[1]
        else:
            fieldName = fieldName[0]
        newField = "coalesce(" + field + ",'')" + " as " + fieldName
        ListOfFieldsNVL.append(newField)

    newListOfFields = ','.join(ListOfFieldsNVL)

    sqlStatement = sqlStatement.replace(oldListOfFields, newListOfFields + " ")
    return sqlStatement
