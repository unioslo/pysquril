
import json

from typing import Union, Callable, Optional, Any

from pysquril.exc import ParseError
from pysquril.parser import (
    Key,
    ArraySpecific,
    ArraySpecificSingle,
    ArraySpecificMultiple,
    ArrayBroadcastSingle,
    ArrayBroadcastMultiple,
    SelectTerm,
    WhereTerm,
    OrderTerm,
    RangeTerm,
    SetTerm,
    Clause,
    UriQuery,
)

class SqlGenerator(object):

    """
    Generic class, used to implement SQL code generation.

    """

    db_init_sql = None
    json_array_sql = None

    def __init__(
        self,
        table_name: str,
        uri_query: str,
        data: Union[list, dict] = None,
        backup_cutoff: Optional[str] = None,
        array_agg: Optional[bool] = False,
        table_name_func: Optional[Callable] = None,
    ) -> None:
        self.table_name = table_name
        self.uri_query = uri_query
        self.data = data
        self.parsed_uri_query = UriQuery(table_name, uri_query)
        self.table_name_func = table_name_func
        self.operators = {
            'eq': '=',
            'gt': '>',
            'gte': '>=',
            'lt': '<',
            'lte': '<=',
            'neq': '!=',
            'like': 'like',
            'ilike': 'ilike',
            'not': 'not',
            'is': 'is',
            'in': 'in'
        }
        if not self.json_array_sql:
            msg = 'Extending the SqlGenerator requires setting the class level property: json_array_sql'
            raise Exception(msg)
        self.has_aggregate_func = False
        self.select_query = self.sql_select(backup_cutoff, array_agg)
        self.update_query = self.sql_update()
        self.delete_query = self.sql_delete()
        self.message = self.uri_message()
        self.alter_query = self.sql_alter()

    # Classes that extend the SqlGenerator must implement the following methods
    # they are called by functions that are mapped over terms in clauses
    # for each term, an appropriate piece of SQL needs to be returned.
    # What is appropriate, depends on the backend.

    def _gen_sql_key_selection(self, term: SelectTerm, parsed: Key) -> str:
        """
        Generate SQL for selecting a Key element.

        Called by _term_to_sql_select when generating the select
        part of the SQL.

        """
        raise NotImplementedError

    def _gen_sql_array_selection(self, term: SelectTerm, parsed: ArraySpecific) -> str:
        """
        Generate SQL for selecting an ArraySpecific element.

        Called by _term_to_sql_select when generating the select
        part of the SQL.

        """
        raise NotImplementedError

    def _gen_sql_array_sub_selection(
        self,
        term: SelectTerm,
        parsed: Union[
            ArraySpecificSingle,
            ArraySpecificMultiple,
            ArrayBroadcastSingle,
            ArrayBroadcastMultiple,
        ],
    ) -> str:
        """
        Generate SQL for selecting inside arrays.

        Called by _term_to_sql_select when generating the select
        part of the SQL.

        """
        raise NotImplementedError

    def _gen_sql_col(self, term: Union[SelectTerm, WhereTerm, OrderTerm]) -> str:
        """
        Generate a column reference from a term,
        used in where and order clauses.

        """
        raise NotImplementedError

    def _gen_sql_update(self, term: Key) -> list:
        """
        Generate a list of update expressions, from a term
        using the data passed to the constructor.

        """
        raise NotImplementedError

    def _clause_map_terms(self, clause: Clause, map_func: Callable) -> list:
        # apply a function to all Terms in a clause
        out = []
        for term in clause.parsed:
            res = map_func(term)
            out.append(res)
        return out

    # methods for mapping functions over terms in different types of clauses

    def select_map(self, map_func: Callable) -> Optional[list]:
        return self._clause_map_terms(self.parsed_uri_query.select, map_func) \
            if self.parsed_uri_query.select else None

    def where_map(self, map_func: Callable) -> Optional[list]:
        return self._clause_map_terms(self.parsed_uri_query.where, map_func) \
            if self.parsed_uri_query.where else None

    def order_map(self, map_func: Callable) -> Optional[list]:
        return self._clause_map_terms(self.parsed_uri_query.order, map_func) \
            if self.parsed_uri_query.order else None

    def range_map(self, map_func: Callable) -> Optional[list]:
        return self._clause_map_terms(self.parsed_uri_query.range, map_func) \
            if self.parsed_uri_query.range else None

    def set_map(self, map_func: Callable) -> Optional[list]:
        return self._clause_map_terms(self.parsed_uri_query.set, map_func) \
            if self.parsed_uri_query.set else None

    def group_by_map(self, map_func: Callable) -> Optional[list]:
        return self._clause_map_terms(self.parsed_uri_query.group_by, map_func) \
            if self.parsed_uri_query.group_by else None

    # term handler functions
    # mapped over terms in a clause
    # generates SQL for each term
    # SQL is generated by calling other functions
    # which are implemented for specific SQL backend implementations

    def _term_to_sql_select(self, term: SelectTerm) -> str:
        rev = term.parsed.copy()
        rev.reverse()
        first_done = False
        for parsed in rev:
            if isinstance(parsed, Key):
                if not first_done:
                    selection = self._gen_sql_key_selection(term, parsed)
            elif isinstance(parsed, ArraySpecific):
                selection = self._gen_sql_array_selection(term, parsed)
            elif (
                isinstance(parsed, ArraySpecificSingle)
                or isinstance(parsed, ArraySpecificMultiple)
                or isinstance(parsed, ArrayBroadcastSingle)
                or isinstance(parsed, ArrayBroadcastMultiple)
            ):
                selection = self._gen_sql_array_sub_selection(term, parsed)
            else:
                raise Exception(f'Could not parse {term.original}')
            first_done = True
        return selection

    def _term_to_sql_where(self, term: WhereTerm) -> str:
        groups_start = ''.join(term.parsed[0].groups_start)
        groups_end = ''.join(term.parsed[0].groups_end)
        combinator = term.parsed[0].combinator if term.parsed[0].combinator else ''
        col = self._gen_sql_col(term)
        op = term.parsed[0].op
        val = term.parsed[0].val
        try:
            int(val)
            val = f"'{val}'" if op in ['eq', 'neq'] else val
        except ValueError:
            if val == 'null' or op == 'in':
                val = f'{val}'
            else:
                val = self._maybe_float(val)
        if op.endswith('.not'):
            op = op.replace('.', ' ')
        elif op.startswith('not.'):
            op = op.replace('.', ' ')
        elif op == 'in':
            val = val.replace('[', '')
            val = val.replace(']', '')
            values = val.split(',')
            new_values = []
            for v in values:
                new = "'%s'" % v
                new_values.append(new)
            joined = ','.join(new_values)
            val = "(%s)" % joined
        else:
            op = self.operators[op]
        if 'like' in op or 'ilike' in op:
            val = val.replace('*', '%')
        out = f'{groups_start} {combinator} {col} {op} {val} {groups_end}'
        return out

    def _term_to_sql_order(self, term: OrderTerm) -> str:
        selection = self._gen_sql_col(term)
        direction = term.parsed[0].direction
        return f'order by {selection} {direction}'

    def _term_to_sql_range(self, term: RangeTerm) -> str:
        return f'limit {term.parsed[0].end} offset {term.parsed[0].start}'

    def _term_to_sql_update(self, term: SelectTerm) -> list:
        return self._gen_sql_update(term)

    # mapper methods - used by public methods

    def _gen_sql_select_clause(self, backup_cutoff: Optional[str] = None) -> str:
        if not backup_cutoff:
            table_reference = self.table_name
        else:
            table_reference = self._gen_select_with_retention(backup_cutoff)
        out = self.select_map(self._term_to_sql_select)
        if not out:
            sql_select = f'select data from {table_reference}'
        else:
            joined = ",".join(out)
            sql_select = f"select {self.json_array_sql}({joined}) data from {table_reference}"
        return sql_select

    def _gen_sql_where_clause(self) -> str:
        out = self.where_map(self._term_to_sql_where)
        if not out:
            sql_where = ''
        else:
            joined = ' '.join(out)
            sql_where = f'where {joined}'
        return sql_where

    def _gen_sql_order_clause(self) -> str:
        out = self.order_map(self._term_to_sql_order)
        if not out:
            return ''
        else:
            return out[0]

    def _gen_sql_range_clause(self) -> str:
        out = self.range_map(self._term_to_sql_range)
        if not out:
            return ''
        else:
            return out[0]

    def _gen_sql_group_by_clause(self) -> str:
        out = self.group_by_map(self._term_to_sql_select)
        if not out:
            return ''
        else:
            cols = ",".join(out)
            return f"group by {cols}"

    def _gen_select_with_retention(self, backup_cutoff: str) -> str:
        raise NotImplementedError

    def _gen_array_agg(self, query: str) -> str:
        raise NotImplementedError

    def _maybe_float(self, val) -> Union[str, float]:
        raise NotImplementedError

    # public methods - called by constructor

    def sql_select(self, backup_cutoff: Optional[str] = None, array_agg: Optional[bool] = False) -> str:
        _select = self._gen_sql_select_clause(backup_cutoff)
        _where = self._gen_sql_where_clause()
        _order = self._gen_sql_order_clause()
        _range = self._gen_sql_range_clause()
        _group_by = self._gen_sql_group_by_clause()
        query = f'{_select} {_where} {_order} {_group_by} {_range}'
        if array_agg and not self.has_aggregate_func:
            return self._gen_array_agg(query)
        else:
            return query

    def sql_update(self) -> str:
        """
        Implementation notes:

        When calling set_map with the _gen_sql_update function
        is called on all set terms, resulting in a list of
        potentential update clauses.

        Sqlite offers json_patch which allows updating
        multiple keys in a JSON structure in one SQL
        statement, negating the need for multiple update
        clauses. The result of set_map is a list of duplicate
        clauses in the case where more than one set term is
        provided.

        Postgres, however, offers jsonb_set, which can
        only be called for one key within a JSON structure
        per SQL statement. The result of set_map is a list
        of unique update clauses.

        Generating valid update statements requires resolving
        the differences in the functionality offered by sqlite
        and postgres.

        The solution is to turn the output of the set_map function
        into a set, removing duplicate entries. This way the sqlite
        variant always returns only one update clause, while the
        postgres variant may return more.

        For postgres the return value from this function is one
        or more update statements, depending on how many JSON keys
        are being changed. For sqlite it is always only one statement.

        """
        out = self.set_map(self._term_to_sql_update)
        if not out:
            return ''
        else:
            _where = self._gen_sql_where_clause()
            _query = ""
            for expr in set(out):
                _query += f"update {self.table_name} {expr} {_where}; "
            return _query

    def sql_delete(self) -> str:
        _where = self._gen_sql_where_clause()
        if not _where:
            query = f"drop table {self.table_name}"
        else:
            query = f"delete from {self.table_name} {_where}"
        return query

    def uri_message(self) -> str:
        return self.parsed_uri_query.message

    def sql_alter(self) -> str:
        """
        Change the name of a table, if it does not
        exist an error is raised.

        """
        alter = self.parsed_uri_query.alter
        if not alter:
            return ""
        else:
            term = alter.parsed[0]
            element = term.parsed[0]
            if self.table_name.endswith('_audit"'):
                new_name = self.table_name_func(f"{element.val}_audit", no_schema=True)
            else:
                new_name = self.table_name_func(element.val, no_schema=True)
            sql = f"alter table {self.table_name} rename to {new_name}"
            return sql


class SqliteQueryGenerator(SqlGenerator):

    """Generate SQL for SQLite json1 backed tables, from a given UriQuery."""

    db_init_sql = None
    json_array_sql = 'json_array'

    # Helper functions - used by mappers

    def _maybe_apply_function(self, term: SelectTerm, selection: str) -> str:
        if not term.func:
            return selection
        elif term.func == 'count':
            if term.original in ['*', '1']:
                selection = '1'
        else:
            if term.func.endswith('_ts'):
                term.func = term.func.replace('_ts', '')
        self.has_aggregate_func = True
        return f"{term.func}({selection})"

    def _gen_sql_key_selection(self, term: SelectTerm, parsed: Key) -> str:
        return self._maybe_apply_function(term, f"json_extract(data, '$.{term.original}')")

    def _gen_sql_array_selection(self, term: SelectTerm, parsed: ArraySpecific) -> str:
        return self._maybe_apply_function(term, f"json_extract(data, '$.{term.original}')")

    def _gen_sql_array_sub_selection(
        self,
        term: SelectTerm,
        parsed: Union[
            ArraySpecificSingle,
            ArraySpecificMultiple,
            ArrayBroadcastSingle,
            ArrayBroadcastMultiple,
        ],
    ) -> str:
        if (
            isinstance(parsed, ArraySpecificSingle)
            or isinstance(parsed, ArraySpecificMultiple)
        ):
            fullkey = f"and fullkey = '$.{term.bare_term}[{parsed.idx}]'"
            vals = 'vals'
        else:
            fullkey = ''
            vals = 'json_group_array(vals)'
        temp = []
        for key in parsed.sub_selections:
            temp.append(f"json_extract(value, '$.{key}')")
        sub_selections = ','.join(temp)
        sub_selections = f'json_array({sub_selections})' if len(temp) > 1 else f'{sub_selections}'
        selection = f"""
                (case when json_extract(data, '$.{term.bare_term}') is not null then (
                    select {vals} from (
                        select
                            {sub_selections} as vals
                        from (
                            select key, value, fullkey, path
                            from {self.table_name}, json_tree({self.table_name}.data)
                            where path = '$.{term.bare_term}'
                            {fullkey}
                            )
                        )
                    )
                else null end)
            """
        return self._maybe_apply_function(term, selection)

    def _gen_sql_col(self, term: Union[SelectTerm, WhereTerm, OrderTerm]) -> str:
        if isinstance(term, WhereTerm) or isinstance(term, OrderTerm):
            select_term = term.parsed[0].select_term
        elif isinstance(term, SelectTerm):
            select_term = term
        if len(select_term.parsed) > 1:
            test_select_term = select_term.parsed[-1]
            if isinstance(test_select_term, ArraySpecific):
                target = select_term.original
            elif isinstance(test_select_term, ArraySpecificSingle):
                _key = select_term.bare_term
                _idx = select_term.parsed[-1].idx
                _col = select_term.parsed[-1].sub_selections[0]
                target = f'{_key}[{_idx}].{_col}'
            else:
                target = select_term.bare_term
        else:
            if not isinstance(select_term.parsed[0], Key):
                raise Exception(f'Invalid term {term.original}')
            target = select_term.parsed[0].element
        col = f"json_extract(data, '$.{target}')"
        if isinstance(term, WhereTerm) and term.parsed[0].op in ['eq', 'neq']:
            col = f"cast ({col} as text)"
        return col

    def _gen_sql_update(self, term: SetTerm) -> str:
        key = term.parsed[0].select_term.bare_term
        if not self.data or key not in self.data.keys():
            raise ParseError(f'Target key of update: {key} not found in payload')
        new = json.dumps(self.data).replace("'", "''")
        return f"set data = json_patch(data, '{new}')"

    def _gen_select_with_retention(self, backup_cutoff: str) -> str:
        return f"(select * from {self.table_name} where json_extract(data, '$.timestamp') >= '{backup_cutoff}')a"

    def _gen_array_agg(self, query: str) -> str:
        return f"select json_group_array(data) from ({query})"

    def _maybe_float(self, val: Any) -> Union[str, float]:
        try:
            float(val)
            if str(float(val)) == str(val):
                return val
            else:
                return f"'{val}'"
        except ValueError:
            return f"'{val}'"


class PostgresQueryGenerator(SqlGenerator):

    json_array_sql = 'jsonb_build_array'
    db_init_sql = [
        """
        create or replace function filter_array_elements(data jsonb, keys text[])
            returns jsonb as $$
            declare key text;
            declare element jsonb;
            declare filtered jsonb;
            declare out jsonb;
            declare val jsonb;
            begin
                create temporary table if not exists info(v jsonb) on commit drop;
                for element in select jsonb_array_elements(data) loop
                    for key in select unnest(keys) loop
                        if filtered is not null then
                            filtered := filtered || jsonb_extract_path(element, key);
                        else
                            filtered := jsonb_extract_path(element, key);
                        end if;
                    if filtered is null then
                        filtered := '[]'::jsonb;
                    end if;
                    end loop;
                insert into info values (filtered);
                filtered := null;
                end loop;
                out := '[]'::jsonb;
                for val in select * from info loop
                    out := out || jsonb_build_array(val);
                end loop;
                return out;
            end;
        $$ language plpgsql;
        """,
        """
        create or replace function unique_data()
        returns trigger as $$
            begin
                NEW.uniq := md5(NEW.data::text);
                return new;
            end;
        $$ language plpgsql;
        """
    ]

    def _maybe_apply_function(self, term: SelectTerm, selection: str) -> str:
        if not term.func:
            return selection
        elif term.func == 'count':
            if term.original in ['*', '1']:
                selection = '1'
        else:
            if term.func in ['avg', 'sum', 'min', 'max']:
                selection = f"({selection})::int"
            if term.func.endswith('_ts'):
                term.func = term.func.replace('_ts', '')
        self.has_aggregate_func = True
        return f"{term.func}({selection})"

    def _gen_select_target(self, term_attr: str) -> str:
        return term_attr.replace('.', ',') if '.' in term_attr else term_attr

    def _gen_sql_key_selection(self, term: SelectTerm, parsed: Key) -> str:
        target = self._gen_select_target(term.original)
        selector = "data#>" if not term.func else "data#>>"
        selection = f"{selector}'{{{target}}}'"
        return self._maybe_apply_function(term, selection)

    def _gen_sql_array_selection(self, term: SelectTerm, parsed: ArraySpecific) -> str:
        target = self._gen_select_target(term.bare_term)
        indexer = "->" if not term.func else "->>"
        selection = f"""
            case when data#>'{{{target}}}'{indexer}{parsed.idx} is not null then
                data#>'{{{target}}}'{indexer}{parsed.idx}
            else null end
            """
        return self._maybe_apply_function(term, selection)

    def _gen_sql_array_sub_selection(
        self,
        term: SelectTerm,
        parsed: Union[
            ArraySpecificSingle,
            ArraySpecificMultiple,
            ArrayBroadcastSingle,
            ArrayBroadcastMultiple,
        ],
    ) -> str:
        target = self._gen_select_target(term.bare_term)
        sub_selections = ','.join(parsed.sub_selections)
        data_selection_expr = f"filter_array_elements(data#>'{{{target}}}','{{{sub_selections}}}')"
        if (
            isinstance(parsed, ArraySpecificSingle)
            or isinstance(parsed, ArraySpecificMultiple)
        ):
            data_selection_expr = f'{data_selection_expr}->{parsed.idx}'
        selection = f"""
            case
                when data#>'{{{target}}}' is not null
                and jsonb_typeof(data#>'{{{target}}}') = 'array'
            then {data_selection_expr}
            else null end
            """
        return self._maybe_apply_function(term, selection)

    def _gen_sql_col(self, term: Union[SelectTerm, WhereTerm, OrderTerm]) -> str:
        if isinstance(term, WhereTerm) or isinstance(term, OrderTerm):
            select_term = term.parsed[0].select_term
        elif isinstance(term, SelectTerm):
            select_term = term
        if isinstance(term, WhereTerm):
            final_select_op = '#>>' # due to integer comparisons
        else:
            final_select_op = '#>'
        if len(select_term.parsed) > 1:
            test_select_term = select_term.parsed[-1]
            if isinstance(test_select_term, ArraySpecific):
                target = self._gen_select_target(select_term.bare_term)
                _idx = select_term.parsed[-1].idx
                col = f"data#>'{{{target}}}'{final_select_op}'{{{_idx}}}'"
            elif isinstance(test_select_term, ArraySpecificSingle):
                target = self._gen_select_target(select_term.bare_term)
                _idx = select_term.parsed[-1].idx
                _col = select_term.parsed[-1].sub_selections[0]
                col = f"data#>'{{{target}}}'#>'{{{_idx}}}'#>'{{{_col}}}'"
            else:
                target = self._gen_select_target(select_term.bare_term)
                col = f"data{final_select_op}'{{{target}}}'"
        else:
            if not isinstance(select_term.parsed[0], Key):
                raise Exception(f'Invalid term {term.original}')
            target = select_term.parsed[0].element
            col = f"data{final_select_op}'{{{target}}}'"
        if isinstance(term, WhereTerm):
            try:
                integer_ops = ['gt', 'gte', 'lt', 'lte']
                int(term.parsed[0].val)
                if (
                    term.parsed[0].op in integer_ops
                    and str(float(term.parsed[0].val)) != str(term.parsed[0].val)
                ):
                    col = f'({col})::int'
                elif str(float(term.parsed[0].val)) == str(term.parsed[0].val):
                    col = f'({col})::real'
            except ValueError:
                pass
        return col

    def _gen_sql_update(self, term: SetTerm) -> str:
        key = term.parsed[0].select_term.bare_term
        if not self.data or key not in self.data.keys():
            raise ParseError(f'Target key of update: {key} not found in payload')
        val = json.dumps(self.data[key]).replace("'", "''") # to handle single quotes inside
        return f" set data = jsonb_set(data, '{{{key}}}', ('{val}')::jsonb)"


    def _gen_select_with_retention(self, backup_cutoff: str) -> str:
        return f"(select * from {self.table_name} where data->>'timestamp' >= '{backup_cutoff}')a"

    def _gen_array_agg(self, query: str) -> str:
        return f"select json_agg(data) from ({query})a"

    def _maybe_float(self, val: Any) -> Union[str, float]:
        return f"'{val}'"
