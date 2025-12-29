# Type 2 Slowly Changing Dimensions in TransmuteDB

TransmuteDB supports Type 2 Slowly Changing Dimensions (SCD Type 2), which track the complete history of changes to dimensional data. This allows you to perform historical analysis and see how data looked at any point in time.

## Overview

Type 2 SCD is a dimensional modeling technique that:
- **Preserves history** by creating a new record for each change
- **Uses business keys** to identify unique dimension members
- **Tracks validity periods** with start and end timestamps
- **Marks current records** with an `_is_current` flag
- **Generates surrogate keys** for stable foreign key relationships

## Quick Start

```python
from transmutedb.flow.entity_builder import (
    update_entity_metadata,
    add_entity_column,
    build_gold_entity
)

# 1. Create Type 2 dimension entity
update_entity_metadata(
    con, "customer", 
    entity_type="type2_dimension"
)

# 2. Add business key and tracked columns
add_entity_column(con, "customer", "customer_id", "INTEGER", 
                 is_business_key=True)
add_entity_column(con, "customer", "customer_name", "VARCHAR", 
                 track_history=True)

# 3. Load and build
load_bronze_entity(con, "customer", source_data)
process_silver_entity(con, "customer")
build_gold_entity(con, "customer")
```

## Key Features

- ✅ Automatic history tracking with business keys
- ✅ Surrogate key generation for stable references
- ✅ Composite business key support
- ✅ Idempotent operations (no-change detection)
- ✅ Configurable column-level tracking
- ✅ Compatible with Kimball methodology

## Example

See the complete working example in `examples/type2_dimension_example.py`.

Run it with:
```bash
python examples/type2_dimension_example.py
```

## Documentation

For detailed documentation, see the tests in `tests/test_type2_dimension.py` which demonstrate:
- Initial dimension load
- Incremental updates with changes
- Adding new dimension members
- Composite business keys
- No-change scenarios

## API Reference

### Entity Metadata
- `entity_type="type2_dimension"` - Enables Type 2 SCD behavior

### Column Metadata
- `is_business_key=True` - Marks column as part of the business key
- `track_history=True` - Tracks changes to this column

### Generated Columns
- `{entity}_key` - Surrogate key (auto-incrementing)
- `_valid_from` - Start of validity period
- `_valid_to` - End of validity period (NULL for current)
- `_is_current` - Boolean flag for current records
- `_row_hash` - Hash for change detection
- `_load_date` - Timestamp of data load
