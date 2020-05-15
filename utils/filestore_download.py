#!/usr/bin/env python
import sys
from trytond.model.fields.binary import Binary
#from trytond.filestore import filestore
from tryton_filestore_s3 import FileStoreS3

dbname = sys.argv[1]
config_file = sys.argv[2]

from trytond.config import config as CONFIG
CONFIG.update_etc(config_file)

from trytond.transaction import Transaction
from trytond.pool import Pool
import logging

Pool.start()
pool = Pool(dbname)
pool.init()

context = {}

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

with Transaction().start(dbname, 0, context=context):
    user_obj = pool.get('res.user')
    user = user_obj.search([('login', '=', 'admin')], limit=1)[0]
    user_id = user.id

with Transaction().start(dbname, 0, context=context) as transaction:

    Invoice = pool.get('account.invoice')
    cursor = transaction.connection.cursor()
    filestore = FileStoreS3()
    for invoice in Invoice.search([('invoice_report_cache','!=', None)]):
        value = invoice.invoice_report_cache
        invoice.invoice_report_cache = None
        basename = filestore.set(value, dbname)
        cursor.execute("UPDATE account_invoice set invoice_report_cache_id='%s', invoice_report_cache=null Where id = %s" % (basename, invoice.id) )
        transaction.commit()


# with Transaction().start(dbname, 0, context=context) as transaction:
#
#     Attachment = pool.get('ir.attachment')
#     cursor = Transaction().cursor
#
#     for attach in Attachment.search([('file_id', "=", None)]):
#         value = attach.data
#         basename = filestore.set(value)
#         cursor.execute("UPDATE ir_attachment set file_id='%s', data=null Where id = %s" % (basename, invoice.id) )
#         cursor.commit()
