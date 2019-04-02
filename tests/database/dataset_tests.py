import tests.database.db_basetest as db_basetest

import imaging_db.database.dataset as dataset


class TestDataset(db_basetest.DBBaseTest):
    def setUp(self):
        super().setUp()

        self.dataset = dataset.DataSet(
            dataset_serial='TEST-2010-01-01-10-00-00-0001',
            description="This is a test dataset",
            microscope='leica',
            frames=True,
            parent_id=None,
        )
        self.session.add(self.dataset)
        self.session.commit()

    def tearDown(self):
        super().tearDown()

    def test_dataset(self):
        expected = [self.dataset]
        result = self.session.query(dataset.DataSet).all()
        self.assertEqual(result, expected)
