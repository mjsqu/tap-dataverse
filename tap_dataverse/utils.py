from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.typing import JSONTypeHelper
from typing import Any, Optional

def attribute_type_to_jsonschema_type(attribute_type: str):
    """
    https://learn.microsoft.com/en-us/power-apps/developer/data-platform/webapi/reference/attributetypecode?view=dataverse-latest
    Name	Value	Description
    Boolean	0	A Boolean attribute.
    Customer	1	An attribute that represents a customer.
    DateTime	2	A date/time attribute.
    Decimal	3	A decimal attribute.
    Double	4	A double attribute.
    Integer	5	An integer attribute.
    Lookup	6	A lookup attribute.
    Memo	7	A memo attribute.
    Money	8	A money attribute.
    Owner	9	An owner attribute.
    PartyList	10	A partylist attribute.
    Picklist	11	A picklist attribute.
    State	12	A state attribute.
    Status	13	A status attribute.
    String	14	A string attribute.
    Uniqueidentifier	15	An attribute that is an ID.
    CalendarRules	16	An attribute that contains calendar rules.
    Virtual	17	An attribute that is created by the system at run time.
    BigInt	18	A big integer attribute.
    ManagedProperty	19	A managed property attribute.
    EntityName	20	An entity name attribute.
    """
    mapping = {
        "Boolean": th.BooleanType,
        "Customer": th.StringType,
        "DateTime": th.DateTimeType,
        "Decimal": th.NumberType,
        "Double": th.NumberType,
        "Integer": th.IntegerType,
        "Lookup": th.StringType,
        "Memo": th.StringType,
        "Money": th.NumberType,
        "Owner": th.StringType,
        "PartyList": th.StringType,
        "Picklist": th.IntegerType,
        "State": th.IntegerType,
        "Status": th.IntegerType,
        "String": th.StringType,
        "Uniqueidentifier": th.UUIDType,
        "CalendarRules": th.StringType,
        "Virtual": th.StringType,
        "BigInt": th.IntegerType,
        "ManagedProperty": th.StringType,
        "EntityName": th.StringType,
    }

    return mapping.get(attribute_type, th.StringType)

def sql_attribute_name(name: str):
    return name.replace('@','__').replace('.','_')