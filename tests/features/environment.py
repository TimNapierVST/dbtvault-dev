from behave.fixture import use_fixture_by_tag

from fixtures import *
from tests.test_utils.dbt_test_utils import *

fixture_registry = {
    "fixture.set_workdir": set_workdir,
    "fixture.single_source_hub": single_source_hub,
    "fixture.sha": sha,
    "fixture.multi_source_hub": multi_source_hub,
    "fixture.single_source_link": single_source_link,
    "fixture.multi_source_link": multi_source_link,
    "fixture.satellite": satellite,
    "fixture.satellite_cycle": satellite_cycle,
    "fixture.t_link": t_link,
    "fixture.cycle": cycle
}


def before_all(context):
    """
    Set up the full test environment and add objects to the context for use in steps
    """

    dbt_test_utils = DBTTestUtils()

    # Setup context
    context.config.setup_logging()
    context.dbt_test_utils = dbt_test_utils

    # Clean dbt folders and generated files
    DBTTestUtils.clean_csv()
    DBTTestUtils.clean_models()
    DBTTestUtils.clean_target()

    # Restore modified YAML to starting state
    DBTVAULTGenerator.clean_test_schema_file()

    # Backup YAML prior to run
    DBTVAULTGenerator.backup_project_yml()

    os.chdir(TESTS_DBT_ROOT)

    context.dbt_test_utils.replace_test_schema()


def before_scenario(context, scenario):
    context.dbt_test_utils.replace_test_schema()


def after_scenario(context, scenario):
    """
    Clean generated files after every scenario
        :param context: behave context
        :param scenario: Current scenario
    """

    DBTTestUtils.clean_csv()
    DBTTestUtils.clean_models()
    DBTTestUtils.clean_target()
    
    DBTVAULTGenerator.clean_test_schema_file()
    DBTVAULTGenerator.restore_project_yml()
    


def before_tag(context, tag):
    if tag.startswith("fixture."):
        return use_fixture_by_tag(tag, context, fixture_registry)
