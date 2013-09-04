from uuid import uuid4

from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.management import sync_table
from cqlengine.management import drop_table
from cqlengine.models import Model
from cqlengine import columns
from cqlengine import ValidationError


class CustomValidationTestModel(Model):
    id      = columns.UUID(primary_key=True, default=lambda: uuid4())
    count   = columns.Integer()
    text    = columns.Text(required=False)
    a_bool  = columns.Boolean(default=False)

    def validate_a_bool(self, val):
        if val:
            raise ValidationError('False only!')
        return val

    def validate_text(self, val):
        if val.lower() == 'jon':
            raise ValidationError('no one likes jon')
        return val


class TestCustomValidation(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestCustomValidation, cls).setUpClass()
        sync_table(CustomValidationTestModel)

    @classmethod
    def tearDownClass(cls):
        super(TestCustomValidation, cls).tearDownClass()
        drop_table(CustomValidationTestModel)

    def test_custom_validation(self):
        # sanity check
        CustomValidationTestModel.create(count=5, text='txt', a_bool=False)

        with self.assertRaises(ValidationError):
            CustomValidationTestModel.create(count=5, text='txt', a_bool=True)

