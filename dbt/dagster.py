from dagster_dbt import dbt_assets, DbtCliResource

dbt_resource = DbtCliResource(
    project_dir="astro_comparison/dbt",
    target="dev"
)

@dbt_assets(
    manifest="astro_comparison/dbt/target/manifest.json",
)
def dbt_analytics(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context)
