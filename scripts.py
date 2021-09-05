import pandas as pd

from sqlalchemy import create_engine


class Settings:
    database_src = "postgresql+psycopg2://vladimir_voronin:q4VCWaLftvDC@analytics.maximum-auto.ru:15432/data"
    sess_columns = ['visitor_session_id', 'site_id', 'visitor_id', 'date_time', 'campaign_id']
    comm_columns = ['communication_id', 'site_id', 'visitor_id', 'date_time']
    result_columns = ['communication_id', 'site_id', 'visitor_id', 'communication_date_time', 'visitor_session_id',
                      'session_date_time', 'campaign_id', 'row_n']


class SqlRequest(Settings):
    """
    table communications:
                            ('communication_id', 'bigint')
                            ('site_id', 'bigint')
                            ('visitor_id', 'bigint')
                            ('date_time', 'timestamp without time zone')

    table sessions:
                            ('visitor_session_id', 'bigint')
                            ('site_id', 'bigint')
                            ('visitor_id', 'bigint')
                            ('date_time', 'timestamp without time zone')
                            ('campaign_id', 'bigint')
    """
    def __init__(self):
        self.engine = create_engine(self.database_src)

    def execute_table(self, request, *args, **kwargs):
        return self.engine.execute(request)

    def get_table(self, table_name: str, columns: list, *args, **kwargs) -> pd.DataFrame:
        data_frame = pd.DataFrame(self.execute_table(f"select * from {table_name};"), columns=columns)
        return data_frame


class Pandas(SqlRequest):
    def __init__(self):
        super().__init__()

    def get_all_table(self, *args, **kwargs):
        return self.get_table('sessions', self.sess_columns), self.get_table('communications', self.comm_columns)

    def generate_table(self) -> pd.DataFrame:
        result = []
        session_table, communication_table = self.get_all_table()
        for i in communication_table.iterrows():
            date = i[1].get('date_time').strftime("%Y-%m-%d %H:%M:%S")
            sess = session_table.query(f'visitor_id=={i[1].get("visitor_id")} & date_time<@date & '
                                       f'site_id=={i[1].get("site_id")}').sort_values(by='date_time')
            if len(sess):
                result.append(
                    [i[1].get('communication_id'), i[1].get("site_id"), i[1].get("visitor_id"), i[1].get('date_time'),
                      sess.tail(1).get('visitor_session_id').values[0], sess.tail(1).get('date_time').values[0],
                      sess.tail(1).get('campaign_id').values[0], len(sess)]
                )
        dat = pd.DataFrame(result, columns=self.result_columns).sort_values(by='communication_id')
        return dat


class SQL(SqlRequest):
    def __init__(self):
        super().__init__()

    def generate_table(self) -> pd.DataFrame:
        data = self.execute_table(
            'select c.communication_id, c.site_id, c.visitor_id, c.date_time communication_date_time, '
            '(select visitor_session_id from sessions '
            'where site_id=c.site_id and visitor_id=c.visitor_id and date_time=max(s.date_time)) visitor_session_id, '
            'MAX(s.date_time) session_date_time, max(s.campaign_id) compaign_id, count(c.communication_id) row_n '
            'from sessions s join communications c ON s.visitor_id = c.visitor_id where s.site_id=c.site_id '
            'and c.date_time>s.date_time group by c.communication_id;'
        )
        return pd.DataFrame(data, columns=self.result_columns)


def check_correct(first_table: pd.DataFrame, second_table: pd.DataFrame) -> bool:
    return not (False in first_table.values == second_table.values)


if __name__ == '__main__':
    pandas_table = Pandas().generate_table()
    sql_table = SQL().generate_table()
    print(f'Таблицы совпадают - {check_correct(pandas_table, sql_table)}\n')
    print(f'Таблица сгенерированная с помощью pandas\n{pandas_table.values}\n')
    print(f'Таблица сгенерированная с помощью sql\n{sql_table.values}')
