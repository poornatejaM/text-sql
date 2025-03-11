# utils/schema.py

def get_updated_schema():
    """Returns the updated schema for the sales_data table."""
    return """
    'Product_ID', 'Int64', '', '', '', '', '\nSale_Date', 'Date', '', '', '', '', '\nSales_Rep', 'String', '', '', '', '',
    '\nRegion', 'String', '', '', '', '', '\nSales_Amount', 'Float64', '', '', '', '', '\nQuantity_Sold', 'Int64', '', '', '', '',
    '\nProduct_Category', 'String', '', '', '', '', '\nUnit_Cost', 'Float64', '', '', '', '', '\nUnit_Price', 'Float64', '', '', '', '',
    '\nCustomer_Type', 'String', '', '', '', '', '\nDiscount', 'Float64', '', '', '', '', '\nPayment_Method', 'String', '', '', '', '',
    '\nSales_Channel', 'String', '', '', '', '', '\nRegion_and_Sales_Rep', 'String', '', '', '', '', ''
    """
