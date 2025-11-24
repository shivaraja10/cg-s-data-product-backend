from pyiceberg.catalog.sql import SqlCatalog

def get_catalog():
    catalog = SqlCatalog(
        name="local",
        uri="sqlite:///catalog.db",          # stores metadata
        warehouse="file:./warehouse"         # folder where data files go
    )
    return catalog
