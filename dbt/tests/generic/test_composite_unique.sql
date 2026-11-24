{% test composite_unique(model, combination_of_columns) %}

select
    {{ combination_of_columns | join(', ') }},
    count(*) as duplicate_count
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1

{% endtest %}
