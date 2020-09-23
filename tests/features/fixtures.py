from behave import fixture

from tests.test_utils.dbt_test_utils import *

"""
The fixtures here are used to supply runtime metadata to tests, in place of metadata usually provided via vars or a YAML config
"""


@fixture
def set_workdir(context):
    """
    Set the working (run) dir for dbt
    """

    os.chdir(TESTS_DBT_ROOT)


@fixture
def sha(context):
    """
    Augment the metadata for a vault structure load to work with SHA hashing instead of MD5
    """

    context.hashing = 'sha'

    if hasattr(context, 'seed_config'):

        config = dict(context.seed_config)

        for k, v in config.items():

            for c, t in config[k]['column_types'].items():

                if t == 'STRING':
                    config[k]['column_types'][c] = 'STRING'

    else:
        raise ValueError('sha fixture used before vault structure fixture.')


@fixture
def single_source_hub(context):
    """
    Define the structures and metadata to load single-source hubs
    """

    context.hash_mapping_config = {
        'RAW_STAGE': {
            'CUSTOMER_PK': 'CUSTOMER_ID'
        }
    }

    context.vault_structure_columns = {
        'HUB': {
            'src_pk': 'CUSTOMER_PK',
            'src_nk': 'CUSTOMER_ID',
            'src_ldts': 'LOADDATE',
            'src_source': 'SOURCE'
        }
    }

    context.seed_config = {
        'HUB': {
            'column_types': {
                'CUSTOMER_PK': 'STRING',
                'CUSTOMER_ID': 'STRING',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'RAW_STAGE': {
            'column_types': {
                'CUSTOMER_ID': 'STRING',
                'CUSTOMER_NAME': 'STRING',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        }
    }


@fixture
def multi_source_hub(context):
    """
    Define the structures and metadata to load multi-source hubs
    """

    context.hash_mapping_config = {
        'RAW_STAGE_PARTS': {
            'PART_PK': 'PART_ID'
        },
        'RAW_STAGE_SUPPLIER': {
            'PART_PK': 'PART_ID',
            'SUPPLIER_PK': 'SUPPLIER_ID'
        },
        'RAW_STAGE_LINEITEM': {
            'PART_PK': 'PART_ID',
            'SUPPLIER_PK': 'SUPPLIER_ID',
            'ORDER_PK': 'ORDER_ID'
        }
    }

    context.vault_structure_columns = {
        'HUB': {
            'src_pk': 'PART_PK',
            'src_nk': 'PART_ID',
            'src_ldts': 'LOADDATE',
            'src_source': 'SOURCE'
        }
    }

    context.seed_config = {
        'HUB': {
            'column_types': {
                'PART_PK': 'STRING',
                'PART_ID': 'STRING',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'RAW_STAGE_PARTS': {
            'column_types': {
                'PART_ID': 'STRING',
                'PART_NAME': 'STRING',
                'PART_TYPE': 'STRING',
                'PART_SIZE': 'STRING',
                'PART_RETAILPRICE': 'NUMERIC',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'RAW_STAGE_SUPPLIER': {
            'column_types': {
                'PART_ID': 'STRING',
                'SUPPLIER_ID': 'STRING',
                'AVAILQTY': 'FLOAT',
                'SUPPLYCOST': 'NUMERIC',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'RAW_STAGE_LINEITEM': {
            'column_types': {
                'ORDER_ID': 'STRING',
                'PART_ID': 'STRING',
                'SUPPLIER_ID': 'STRING',
                'LINENUMBER': 'FLOAT',
                'QUANTITY': 'FLOAT',
                'EXTENDED_PRICE': 'NUMERIC',
                'DISCOUNT': 'NUMERIC',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        }
    }


@fixture
def single_source_link(context):
    """
    Define the structures and metadata to load single-source links
    """

    context.hash_mapping_config = {
        'RAW_STAGE': {
            'CUSTOMER_NATION_PK': ['CUSTOMER_ID', 'NATION_ID'],
            'CUSTOMER_FK': 'CUSTOMER_ID',
            'NATION_FK': 'NATION_ID'
        }
    }

    context.vault_structure_columns = {
        'LINK': {
            'src_pk': 'CUSTOMER_NATION_PK',
            'src_fk': ['CUSTOMER_FK', 'NATION_FK'],
            'src_ldts': 'LOADDATE',
            'src_source': 'SOURCE'
        }
    }

    context.seed_config = {
        'LINK': {
            'column_types': {
                'CUSTOMER_NATION_PK': 'STRING',
                'CUSTOMER_FK': 'STRING',
                'NATION_FK': 'STRING',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'RAW_STAGE': {
            'column_types': {
                'CUSTOMER_ID': 'STRING',
                'NATION_ID': 'STRING',
                'CUSTOMER_NAME': 'STRING',
                'CUSTOMER_DOB': 'DATE',
                'CUSTOMER_PHONE': 'STRING',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        }
    }


@fixture
def multi_source_link(context):
    """
    Define the structures and metadata to load single-source links
    """

    context.hash_mapping_config = {
        'RAW_STAGE_SAP': {
            'CUSTOMER_NATION_PK': ['CUSTOMER_ID', 'NATION_ID'],
            'CUSTOMER_FK': 'CUSTOMER_ID',
            'NATION_FK': 'NATION_ID'
        },
        'RAW_STAGE_CRM': {
            'CUSTOMER_NATION_PK': ['CUSTOMER_ID', 'NATION_ID'],
            'CUSTOMER_FK': 'CUSTOMER_ID',
            'NATION_FK': 'NATION_ID'
        },
        'RAW_STAGE_WEB': {
            'CUSTOMER_NATION_PK': ['CUSTOMER_ID', 'NATION_ID'],
            'CUSTOMER_FK': 'CUSTOMER_ID',
            'NATION_FK': 'NATION_ID'
        },
    }

    context.vault_structure_columns = {
        'LINK': {
            'src_pk': 'CUSTOMER_NATION_PK',
            'src_fk': ['CUSTOMER_FK', 'NATION_FK'],
            'src_ldts': 'LOADDATE',
            'src_source': 'SOURCE'
        }
    }

    context.seed_config = {
        'LINK': {
            'column_types': {
                'CUSTOMER_NATION_PK': 'STRING',
                'CUSTOMER_FK': 'STRING',
                'NATION_FK': 'STRING',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'RAW_STAGE_SAP': {
            'column_types': {
                'CUSTOMER_ID': 'STRING',
                'NATION_ID': 'STRING',
                'CUSTOMER_NAME': 'STRING',
                'CUSTOMER_DOB': 'DATE',
                'CUSTOMER_PHONE': 'STRING',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'RAW_STAGE_CRM': {
            'column_types': {
                'CUSTOMER_ID': 'STRING',
                'NATION_ID': 'STRING',
                'CUSTOMER_NAME': 'STRING',
                'CUSTOMER_DOB': 'DATE',
                'CUSTOMER_PHONE': 'STRING',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'RAW_STAGE_WEB': {
            'column_types': {
                'CUSTOMER_ID': 'STRING',
                'NATION_ID': 'STRING',
                'CUSTOMER_NAME': 'STRING',
                'CUSTOMER_DOB': 'DATE',
                'CUSTOMER_PHONE': 'STRING',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        }
    }


@fixture
def satellite(context):
    """
    Define the structures and metadata to load satellites
    """

    context.hash_mapping_config = {
        'RAW_STAGE': {
            'CUSTOMER_PK': 'CUSTOMER_ID',
            'HASHDIFF': {'is_hashdiff': True,
                         'columns': ['CUSTOMER_ID', 'CUSTOMER_DOB', 'CUSTOMER_PHONE', 'CUSTOMER_NAME']}
        }
    }

    context.derived_mapping = {
        'RAW_STAGE': {
            'EFFECTIVE_FROM': 'LOADDATE'
        }
    }

    context.vault_structure_columns = {
        'SATELLITE': {
            'src_pk': 'CUSTOMER_PK',
            'src_payload': ['CUSTOMER_NAME', 'CUSTOMER_PHONE', 'CUSTOMER_DOB'],
            'src_hashdiff': 'HASHDIFF',
            'src_eff': 'EFFECTIVE_FROM',
            'src_ldts': 'LOADDATE',
            'src_source': 'SOURCE'
        }
    }

    context.seed_config = {
        'RAW_STAGE': {
            'column_types': {
                'CUSTOMER_ID': 'NUMERIC',
                'CUSTOMER_NAME': 'STRING',
                'CUSTOMER_PHONE': 'STRING',
                'CUSTOMER_DOB': 'DATE',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'SATELLITE': {
            'column_types': {
                'CUSTOMER_PK': 'STRING',
                'CUSTOMER_NAME': 'STRING',
                'CUSTOMER_PHONE': 'STRING',
                'CUSTOMER_DOB': 'DATE',
                'HASHDIFF': 'STRING',
                'EFFECTIVE_FROM': 'DATE',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        }
    }


@fixture
def satellite_cycle(context):
    """
    Define the structures and metadata to perform load cycles for satellites
    """

    context.hash_mapping_config = {
        'RAW_STAGE':
            {'CUSTOMER_PK': 'CUSTOMER_ID',
             'HASHDIFF': {'is_hashdiff': True,
                          'columns': ['CUSTOMER_DOB', 'CUSTOMER_ID', 'CUSTOMER_NAME']
                          }
             }
    }

    context.derived_mapping = {
        'RAW_STAGE': {
            'EFFECTIVE_FROM': 'LOADDATE'
        }
    }

    context.stage_columns = {
        'RAW_STAGE':
            ['CUSTOMER_ID',
             'CUSTOMER_NAME',
             'CUSTOMER_DOB',
             'EFFECTIVE_FROM',
             'LOADDATE',
             'SOURCE']
    }

    context.vault_structure_columns = {
        'SATELLITE': {
            'src_pk': 'CUSTOMER_PK',
            'src_payload': ['CUSTOMER_NAME', 'CUSTOMER_DOB'],
            'src_hashdiff': 'HASHDIFF',
            'src_eff': 'EFFECTIVE_FROM',
            'src_ldts': 'LOADDATE',
            'src_source': 'SOURCE'
        }
    }

    context.seed_config = {
        'RAW_STAGE': {
            'column_types': {
                'CUSTOMER_ID': 'STRING',
                'CUSTOMER_NAME': 'STRING',
                'CUSTOMER_DOB': 'DATE',
                'EFFECTIVE_FROM': 'DATE',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'SATELLITE': {
            'column_types': {
                'CUSTOMER_PK': 'STRING',
                'CUSTOMER_NAME': 'STRING',
                'CUSTOMER_DOB': 'DATE',
                'HASHDIFF': 'STRING',
                'EFFECTIVE_FROM': 'DATE',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        }
    }


@fixture
def t_link(context):
    """
    Define the structures and metadata to perform load cycles for transactional links
    """

    context.hash_mapping_config = {
        'RAW_STAGE': {
            'TRANSACTION_PK': ['CUSTOMER_ID', 'TRANSACTION_NUMBER'],
            'CUSTOMER_FK': 'CUSTOMER_ID'
        }
    }

    context.derived_mapping = {
        'RAW_STAGE': {
            'EFFECTIVE_FROM': 'TRANSACTION_DATE'
        }
    }

    context.vault_structure_columns = {
        'T_LINK': {
            'src_pk': 'TRANSACTION_PK',
            'src_fk': 'CUSTOMER_FK',
            'src_payload': ['TRANSACTION_NUMBER', 'TRANSACTION_DATE',
                            'TYPE', 'AMOUNT'],
            'src_eff': 'EFFECTIVE_FROM',
            'src_ldts': 'LOADDATE',
            'src_source': 'SOURCE'
        }
    }

    context.seed_config = {
        'RAW_STAGE': {
            'column_types': {
                'CUSTOMER_ID': 'STRING',
                'TRANSACTION_NUMBER': 'NUMERIC',
                'TRANSACTION_DATE': 'DATE',
                'TYPE': 'STRING',
                'AMOUNT': 'NUMERIC',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'T_LINK': {
            'column_types': {
                'TRANSACTION_PK': 'STRING',
                'CUSTOMER_FK': 'STRING',
                'TRANSACTION_NUMBER': 'NUMERIC',
                'TRANSACTION_DATE': 'DATE',
                'TYPE': 'STRING',
                'AMOUNT': 'NUMERIC',
                'EFFECTIVE_FROM': 'DATE',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        }
    }


@fixture
def cycle(context):
    """
    Define the structures and metadata to perform vault load cycles
    """

    context.hash_mapping_config = {
        'RAW_STAGE_CUSTOMER': {
            'CUSTOMER_PK': 'CUSTOMER_ID',
            'HASHDIFF': {'is_hashdiff': True,
                         'columns': ['CUSTOMER_DOB', 'CUSTOMER_ID', 'CUSTOMER_NAME']
                         }
        },
        'RAW_STAGE_BOOKING': {
            'CUSTOMER_PK': 'CUSTOMER_ID',
            'BOOKING_PK': 'BOOKING_ID',
            'CUSTOMER_BOOKING_PK': ['CUSTOMER_ID', 'BOOKING_ID'],
            'HASHDIFF_BOOK_CUSTOMER_DETAILS': {'is_hashdiff': True,
                                               'columns': ['CUSTOMER_ID',
                                                           'NATIONALITY',
                                                           'PHONE']
                                               },
            'HASHDIFF_BOOK_BOOKING_DETAILS': {'is_hashdiff': True,
                                              'columns': ['BOOKING_ID',
                                                          'BOOKING_DATE',
                                                          'PRICE',
                                                          'DEPARTURE_DATE',
                                                          'DESTINATION']
                                              }
        }
    }

    context.derived_mapping = {
        'RAW_STAGE_CUSTOMER': {
            'EFFECTIVE_FROM': 'LOADDATE'
        },
        'RAW_STAGE_BOOKING': {
            'EFFECTIVE_FROM': 'BOOKING_DATE'
        }
    }

    context.vault_structure_columns = {
        'HUB_CUSTOMER': {
            'source_model': ['raw_stage_customer_hashed',
                             'raw_stage_booking_hashed'],
            'src_pk': 'CUSTOMER_PK',
            'src_nk': 'CUSTOMER_ID',
            'src_ldts': 'LOADDATE',
            'src_source': 'SOURCE'
        },
        'HUB_BOOKING': {
            'source_model': 'raw_stage_booking_hashed',
            'src_pk': 'BOOKING_PK',
            'src_nk': 'BOOKING_ID',
            'src_ldts': 'LOADDATE',
            'src_source': 'SOURCE'
        },
        'LINK_CUSTOMER_BOOKING': {
            'source_model': 'raw_stage_booking_hashed',
            'src_pk': 'CUSTOMER_BOOKING_PK',
            'src_fk': ['CUSTOMER_PK', 'BOOKING_PK'],
            'src_ldts': 'LOADDATE',
            'src_source': 'SOURCE'
        },
        'SAT_CUST_CUSTOMER_DETAILS': {
            'source_model': 'raw_stage_customer_hashed',
            'src_pk': 'CUSTOMER_PK',
            'src_hashdiff': 'HASHDIFF',
            'src_payload': ['CUSTOMER_NAME', 'CUSTOMER_DOB'],
            'src_eff': 'EFFECTIVE_FROM',
            'src_ldts': 'LOADDATE',
            'src_source': 'SOURCE'
        },
        'SAT_BOOK_CUSTOMER_DETAILS': {
            'source_model': 'raw_stage_booking_hashed',
            'src_pk': 'CUSTOMER_PK',
            'src_hashdiff': {'source_column': 'HASHDIFF_BOOK_CUSTOMER_DETAILS',
                             'alias': 'HASHDIFF'},
            'src_payload': ['PHONE', 'NATIONALITY'],
            'src_eff': 'EFFECTIVE_FROM',
            'src_ldts': 'LOADDATE',
            'src_source': 'SOURCE'
        },
        'SAT_BOOK_BOOKING_DETAILS': {
            'source_model': 'raw_stage_booking_hashed',
            'src_pk': 'BOOKING_PK',
            'src_hashdiff': {'source_column': 'HASHDIFF_BOOK_BOOKING_DETAILS',
                             'alias': 'HASHDIFF'},
            'src_payload': ['PRICE', 'BOOKING_DATE',
                            'DEPARTURE_DATE', 'DESTINATION'],
            'src_eff': 'EFFECTIVE_FROM',
            'src_ldts': 'LOADDATE',
            'src_source': 'SOURCE'
        }
    }

    context.stage_columns = {
        'RAW_STAGE_CUSTOMER':
            ['CUSTOMER_ID',
             'CUSTOMER_NAME',
             'CUSTOMER_DOB',
             'EFFECTIVE_FROM',
             'LOADDATE',
             'SOURCE']
        ,
        'RAW_STAGE_BOOKING':
            ['BOOKING_ID',
             'CUSTOMER_ID',
             'BOOKING_DATE',
             'PRICE',
             'DEPARTURE_DATE',
             'DESTINATION',
             'PHONE',
             'NATIONALITY',
             'LOADDATE',
             'SOURCE']
    }

    context.seed_config = {
        'RAW_STAGE_CUSTOMER': {
            'column_types': {
                'CUSTOMER_ID': 'STRING',
                'CUSTOMER_NAME': 'STRING',
                'CUSTOMER_DOB': 'DATE',
                'EFFECTIVE_FROM': 'DATE',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'RAW_STAGE_BOOKING': {
            'column_types': {
                'BOOKING_ID': 'STRING',
                'CUSTOMER_ID': 'STRING',
                'PRICE': 'NUMERIC',
                'DEPARTURE_DATE': 'DATE',
                'BOOKING_DATE': 'DATE',
                'PHONE': 'STRING',
                'DESTINATION': 'STRING',
                'NATIONALITY': 'STRING',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'HUB_CUSTOMER': {
            'column_types': {
                'CUSTOMER_PK': 'STRING',
                'CUSTOMER_ID': 'NUMERIC',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'HUB_BOOKING': {
            'column_types': {
                'BOOKING_PK': 'STRING',
                'BOOKING_ID': 'NUMERIC',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'LINK_CUSTOMER_BOOKING': {
            'column_types': {
                'CUSTOMER_BOOKING_PK': 'STRING',
                'CUSTOMER_PK': 'STRING',
                'BOOKING_PK': 'STRING',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'SAT_CUST_CUSTOMER_DETAILS': {
            'column_types': {
                'CUSTOMER_PK': 'STRING',
                'HASHDIFF': 'STRING',
                'CUSTOMER_NAME': 'STRING',
                'CUSTOMER_DOB': 'DATE',
                'EFFECTIVE_FROM': 'DATE',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'SAT_BOOK_CUSTOMER_DETAILS': {
            'column_types': {
                'CUSTOMER_PK': 'STRING',
                'HASHDIFF': 'STRING',
                'PHONE': 'STRING',
                'NATIONALITY': 'STRING',
                'EFFECTIVE_FROM': 'DATE',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        },
        'SAT_BOOK_BOOKING_DETAILS': {
            'column_types': {
                'BOOKING_PK': 'STRING',
                'HASHDIFF': 'STRING',
                'PRICE': 'NUMERIC',
                'BOOKING_DATE': 'DATE',
                'DEPARTURE_DATE': 'DATE',
                'DESTINATION': 'STRING',
                'EFFECTIVE_FROM': 'DATE',
                'LOADDATE': 'DATE',
                'SOURCE': 'STRING'
            }
        }
    }
