REFRESH MATERIALIZED VIEW IDENTIFIER(CONCAT({{catalog}}, '.', {{schema_gold}}, '.dim_user'));

REFRESH MATERIALIZED VIEW IDENTIFIER(CONCAT({{catalog}}, '.', {{schema_gold}}, '.dim_channel'));

REFRESH MATERIALIZED VIEW IDENTIFIER(CONCAT({{catalog}}, '.', {{schema_gold}}, '.dim_date'));

REFRESH MATERIALIZED VIEW IDENTIFIER(CONCAT({{catalog}}, '.', {{schema_gold}}, '.fact_transactions'));

REFRESH MATERIALIZED VIEW IDENTIFIER(CONCAT({{catalog}}, '.', {{schema_gold}}, '.dim_merchant'));