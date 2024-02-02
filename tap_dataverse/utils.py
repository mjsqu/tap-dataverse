from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.typing import JSONTypeHelper


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
        "Picklist": th.StringType,
        "State": th.StringType,
        "Status": th.StringType,
        "String": th.StringType,
        "Uniqueidentifier": th.StringType,
        "CalendarRules": th.StringType,
        "Virtual": th.StringType,
        "BigInt": th.StringType,
        "ManagedProperty": th.StringType,
        "EntityName": th.StringType,
    }

    return mapping.get(attribute_type,th.StringType)

def attribute_to_properties(attribute: dict) -> [th.Property]:
        """
        TODO: Handle this:
        "_owningbusinessunit_value@OData.Community.Display.V1.FormattedValue": "ipmhadev",
        "_owningbusinessunit_value@Microsoft.Dynamics.CRM.associatednavigationproperty": "owningbusinessunit",
        "_owningbusinessunit_value@Microsoft.Dynamics.CRM.lookuplogicalname": "businessunit",
        """
        SIMPLE = []
        FORMATTED = []
        FORMATTED_NAV_LKUP = []
        
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
        "Picklist": th.StringType,
        "State": th.StringType,
        "Status": th.StringType,
        "String": th.StringType,
        "Uniqueidentifier": th.StringType,
        "CalendarRules": th.StringType,
        "Virtual": th.StringType,
        "BigInt": th.StringType,
        "ManagedProperty": th.StringType,
        "EntityName": th.StringType,
    }