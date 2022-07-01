
import boto3

rules = [
    {
        "type": "column",
        "column_name": "order_status",
        "operation": "in",
        "values": [
            "begin",
            "shpping",
            "paied",
            "cancel"
        ]
    },
    {
        "type": "table",
        "metric_name": "rows_count",
        "operation": ">",
        "values": 1000
    }
]
IS_NOT_NULL = 'is not null'
IS_UNIQUE = 'is unique'
OP_IN = 'in'
OP_BIGGER = '>'


def is_unque(column_name, operation) -> str:
    if operation == IS_UNIQUE:
        return f'.isUnique("{column_name}")'
    return None


def is_not_null(column_name, operation) -> str:
    if operation == IS_NOT_NULL:
        return f'.isComplete("{column_name}")'
    return None


def is_in(column_name, operation, values, schema: dict) -> str:
    if operation == OP_IN:
        v = ['"'+item+'"' for item in values]
        str_array = 'Array(' + ','.join(v) + ')'
        return f'.isContainedIn("{column_name}",{str_array})'
    return None


def bigger_than(column_name, operation, values, schema: dict) -> str:
    if operation == OP_BIGGER:
        f'.hasMin("{column_name}",{values})'
    return None


duality_handles = list()
duality_handles.append(is_unque)
duality_handles.append(is_not_null)

three_handles = list()
three_handles.append(is_in)


def build():
    code = list()
    code.append("import com.amazon.deequ.VerificationSuite")
    code.append(
        "import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}")
    code.append("\n\n")
    code.append("val verificationResult = VerificationSuite()")

    tb_schema = dict()
    for rule in rules:
        if rule['type'] == 'columns':
            operation = rule['operation']
            column_name = rule['column_name']
            # 二元操作
            if operation in [IS_NOT_NULL, IS_UNIQUE]:
                for method in duality_handles:
                    code_line = method(column_name, operation)
                    if code_line:
                        code.append(code_line)
                        continue

            else:
                values = rule['values']
                for method in duality_handles:
                    code_line = method(
                        column_name, operation, values, tb_schema)
                    if code_line:
                        code.append(code_line)
                        continue
