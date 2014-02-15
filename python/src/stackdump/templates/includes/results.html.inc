{% import 'common.macros' as macros %}

{% if results|length == 0 %}
<p><em>No questions or answers found for your query.</em></p>
{% if site and sites|list|length > 1 %}
<p>Try searching in another StackExchange website by using the links on the right.</p>
{% endif %}
{% else %}
<ul id="search-results">
    {% for r in results %}
    <li>
        {% if not site %}
        <div class="post-logo">
            <img src="{{ SETTINGS.APP_URL_ROOT }}media/logos/{{ r.site.key }}.png" alt="{{ r.site.name }} logo" title="{{ r.site.name }} logo" />
        </div>
        {% endif %}
        <div class="post-stats-vertical">
            <div class="post-stat">
                <p class="post-stat-value {% if r.question.score < 0 %}post-stat-value-poor{% endif %}">
                    {{ r.question.score }}
                </p>
                <p>vote{% if r.question.score != 1 %}s{% endif %}</p>
            </div>
            <div class="post-stat">
                <p class="post-stat-value {% if r.answers|length == 0 %}post-stat-value-poor{% endif %} {% if r.question.acceptedAnswerId %}post-stat-value-good{% endif %}">
                    {% if r.question.acceptedAnswerId %}
                    <span title="One of the answers has been accepted as the correct answer.">{{ r.answers|length }}</span>
                    {% else %}
                    {{ r.answers|length }}
                    {% endif %}
                </p>
                <p>answer{% if r.answers|length != 1 %}s{% endif %}</p>
            </div>
        </div>
        <div class="post-summary {% if not site %}post-summary-with-logo{% endif %}">
            <h3><a href="{{ SETTINGS.APP_URL_ROOT }}{{ r.site.key }}/{{ r.id }}">{{ r.question.title }}</a></h3>
            <p>{{ r.question.body|striptags|truncate(256) }}</p>
            <p class="post-details">
                Asked by <strong>{{ macros.user_name(r.question.ownerUser) }}</strong> on
                <strong>{{ r.question.creationDate|format_datetime }}</strong>.
            </p>
            <div class="post-tags">
                {% for t in r.question.tags %}
                <span class="label">{{ t }}</span>
                {% endfor %}
            </div>
        </div>
        {# hack to force a clear on all internal elements #}
        <div class="clearfix"></div>
    </li>
    {% endfor %}
</ul>
{% if total_pages and total_pages > 1 %}
<div class="pagination">
    <ul>
        {% if current_page > 1 %}
        {# the prev page is current_page - 2 because current_page is ones-based, but the p GET parameter is zero-based #}
        <li class="prev"><a href="{{ REQUEST.url|set_get_parameters('p=' ~ (current_page - 2)) }}">&larr; Previous</a></li>
        {% else %}
        <li class="prev disabled"><a href="#">&larr; Previous</a></li>
        {% endif %}
        
        {% set page_numbers = total_pages|get_surrounding_page_numbers(current_page) %}
        {% if page_numbers|first != 1 %}
        <li class="disabled"><a href="#">...</a></li>
        {% endif %}
        {% for p in page_numbers %}
        <li {% if p == current_page %}class="active"{% endif %}>
            {% if p == current_page %}
            {# this extra a tag is a hack to make bootstrap render properly. #}
            <a>{{ p }}</a>
            {% else %}
            <a href="{{ REQUEST.url|set_get_parameters('p=' ~ (p-1)) }}">{{ p }}</a>
            {% endif %}
        </li>
        {% endfor %}
        {% if page_numbers|last != total_pages %}
        <li class="disabled"><a href="#">...</a></li>
        {% endif %}
        
        {% if current_page != total_pages %}
        {# the next page is just current_page because current_page is ones-based, but the p GET parameter is zero-based #}
        <li class="next"><a href="{{ REQUEST.url|set_get_parameters('p=' ~ current_page) }}">&rarr; Next</a></li>
        {% else %}
        <li class="next disabled"><a href="#">&rarr; Next</a></li>
        {% endif %}
    </ul>
</div>
{% endif %}
{% endif %}