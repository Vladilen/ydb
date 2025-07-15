from .base import DeleteTestBase
import pytest
import random
import sys


class TestDeleteAllAfterInserts(DeleteTestBase):
    test_name = "delete_by_explicit_row_id"
    MAX_ID = 2**32 // 2 - 1

    @classmethod
    def setup_class(cls):
        super(TestDeleteAllAfterInserts, cls).setup_class()

    def _get_test_dir(self):
        return f"{self.ydb_client.database}/{self.test_name}"

    def _get_row_count(self, table_path):
        return self.ydb_client.query(f"SELECT count(*) as Rows from `{table_path}`")[0].rows[0]["Rows"]

    def _test_two_columns_pk(self, rows_to_insert, insert_iterations):
        table_path = f"{self._get_test_dir()}/testTableAllDeleted"
        self.ydb_client.query(f"DROP TABLE IF EXISTS `{table_path}`;")
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id1 Int32 NOT NULL,
                value Int64,
                PRIMARY KEY(id1),
            )
            WITH (
                STORE = COLUMN
            )
            """
        )

        all_rows_ids = random.sample(range(self.MAX_ID), rows_to_insert * insert_iterations)
        rows_in_table = 0
        for it in range(0, len(all_rows_ids), rows_to_insert):
            rows_ids = all_rows_ids[it:it + rows_to_insert]

            insert_query = f"INSERT INTO `{table_path}` (id1, value) VALUES "
            for i in rows_ids:
                insert_query += f"({i}, {i}), "
            insert_query = insert_query[:-2] + ";"
            self.ydb_client.query(insert_query)

            rows_in_table += rows_to_insert
            # assert self._get_row_count(table_path) == rows_in_table

        self.ydb_client.query(f"DELETE FROM `{table_path}`")

        # assert self._get_row_count(table_path) == 0

        # # passes
        # assert len(self.ydb_client.query(
        #     f"""
        #     SELECT id1 as id1, id2 as id2 FROM `{table_path}` WHERE id1 != 10000000 ORDER by id1, id2 LIMIT 100;
        #     """
        # )[0].rows) == 0

        # # passes
        # assert len(self.ydb_client.query(
        #     f"""
        #     SELECT id1 as id1, id2 as id2 FROM `{table_path}` WHERE id1 != 10000000 ORDER by id1 DESC, id2 DESC;
        #     """
        # )[0].rows) == 0

        # # passes
        # assert len(self.ydb_client.query(
        #     f"""
        #     SELECT id1 as id1, id2 as id2 FROM `{table_path}` WHERE id1 != 10000000 LIMIT 100;
        #     """
        # )[0].rows) == 0

        # # passes
        # assert len(self.ydb_client.query(
        #     f"""
        #     SELECT id1 as id1, id2 as id2 FROM `{table_path}` WHERE id1 != 10000000;
        #     """
        # )[0].rows) == 0

        # fails
        assert len(self.ydb_client.query(
            f"""
            SELECT id1 as id1 FROM `{table_path}` WHERE id1 != 10000000 ORDER by id1 DESC LIMIT 5;
            """
        )[0].rows) == 0

    def _test_upsert_two_columns_pk(self, rows_to_insert, insert_iterations):
        table_path = f"{self._get_test_dir()}/testTableAllDeleted"
        self.ydb_client.query(f"DROP TABLE IF EXISTS `{table_path}`;")
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id1 Int32 NOT NULL,
                value Utf8 NOT NULL,
                PRIMARY KEY(id1),
            )
            WITH (
                STORE = COLUMN

            )
            """
                            # AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
        )
        # all_rows_ids = random.sample(range(self.MAX_ID), rows_to_insert * insert_iterations)
        all_rows_ids = [945294066, 1142679124, 175083513, 1218666029]
        print(all_rows_ids, file=sys.stderr)
        rows_in_table = 0
        for it in range(0, len(all_rows_ids), rows_to_insert):
            print("INSERT", file=sys.stderr)
            rows_ids = all_rows_ids[it:it + rows_to_insert]

            insert_query = f"INSERT INTO `{table_path}` (id1, value) VALUES "
            for i in rows_ids:
                insert_query += f"({i}, 'OLD'), "
            insert_query = insert_query[:-2] + ";"
            self.ydb_client.query(insert_query)

            rows_in_table += rows_to_insert
            # assert self._get_row_count(table_path) == rows_in_table

        print("UPDATE", file=sys.stderr)
        self.ydb_client.query(f"UPDATE `{table_path}` SET value = 'NEW';")

        # assert self._get_row_count(table_path) == rows_in_table

        # print(rows_in_table, file=sys.stderr)
        # # passes
        # res = self.ydb_client.query(
        #     f"""
        #     SELECT id1 as id1, value as value FROM `{table_path}` WHERE id1 != 10000001 ORDER by id1 DESC;
        #     """
        # )[0].rows

        # for row in res:
        #     assert row["value"] == row["id1"] + 5

        # fails
        # res1 = self.ydb_client.query(
        #     f"""
        #     SELECT id1 as id1, value as value FROM `{table_path}` WHERE id1 != 10000001 ORDER by id1 DESC;
        #     """
        # )[0].rows

        print("SELECT", file=sys.stderr)
        res = self.ydb_client.query(
            f"""
            SELECT id1 as id1, value as value FROM `{table_path}` WHERE id1 != 10000001 ORDER by id1 DESC LIMIT 100;
            """
        )[0].rows

        print("DONE", file=sys.stderr)
        # prev_row = None
        for i in range(len(res)):
            row = res[i]
            # print(row, file=sys.stderr)
            assert row["value"] == 'NEW'
            # if prev_row is not None:
            #     assert row["id1"] < prev_row["id1"]
            # prev_row = row
            # assert row["id1"] == res1[i]["id1"]

    # @pytest.mark.skip(reason="https://github.com/ydb-platform/ydb/issues/20098")
    def test_delete_all_rows_after_several_inserts(self):
        # IMPORTANT note: tests passes with 1 insert_iterations
        self._test_two_columns_pk(rows_to_insert=10, insert_iterations=2)

    def test_upsert_rows_after_several_inserts(self):
        # IMPORTANT note: tests passes with 1 insert_iterations
        # while True:
        self._test_upsert_two_columns_pk(rows_to_insert=2, insert_iterations=2)
