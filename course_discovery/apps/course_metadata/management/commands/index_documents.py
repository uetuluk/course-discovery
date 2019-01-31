from pydoc import locate

from django.conf import settings
from django.core.management.base import BaseCommand

from elasticsearch_dsl.connections import connections as connectionsDsl

from course_discovery.apps.core.utils import ElasticsearchUtils


class Command(BaseCommand):

    def __init__(self):
        super(Command, self).__init__()
        self.indexes = []
        self.connections = settings.ES_CONNECTIONS
        indexes = settings.ES_INDEXES
        for name, value in self.connections.items():
            for index_name, index_classes in indexes.get(name):
                self.indexes.append({
                    'connection_name': name,
                    'connection': value,
                    'index_name': index_name,
                    'index_classes': index_classes,
                })

    def add_arguments(self, parser):
        parser.add_argument(
            '-b', '--batch-size', dest='batch_size', type=int,
            help='Number of items to index at once.'
        )
        parser.add_argument(
            '-r', '--remove', action='store_true', default=False,
            help='Remove objects from the index that are no longer present in \
                  the database.'
        )
        parser.add_argument(
            '-i', '--index', dest='index', type=str,
            help='Specify which index to update.'
        )
        parser.add_argument(
            '-c', '--clear_index', action='store_true', default=False,
            help='Clear and rebuild index.'
        )
        parser.add_argument(
            '-a', '--age', dest='age', default=0,
            help='Number of hours back to consider objects new.'
        )

    def handle(self, *args, **options):

        index = options.get('index')
        clear_index = options.get('clear_index')

        self.batch_size = options.get('batch_size')
        self.remove = options.get('remove')
        self.age = options.get('age')

        indexes = self.get_indexes(index)

        for index in indexes:
            if clear_index:
                self.clear_index(index)
            self.index_documents(index)

    def get_indexes(self, index):
        indexes = self.indexes
        if index:
            indexes = filter(
                lambda x: x['index_name'] == index,
                indexes
            )
        return indexes

    def clear_index(self, index):
        IndexClass = locate(index['index_class'])
        index_obj = IndexClass()
        index_obj.clear_index()

    def index_documents(self, index):
        self.create_new_index(index)
        for index_class in index['index_classes']:
            import pdb; pdb.set_trace()
            IndexClass = locate(index_class)
            IndexClass.index_documents(
                index, self.batch_size, self.remove, self.age)

    @staticmethod
    def create_new_index(index_config):
        es = connectionsDsl.create_connection(
            hosts=index_config['connection']['hosts']
        )
        alias = index_config['index_name']
        index_name = ElasticsearchUtils.create_index(es, alias, 'ES-DSL')

        body = {
            'actions': [
                {'remove': {'alias': alias, 'index': '*'}},
                {'add': {'alias': alias, 'index': index_name}},
            ]
        }
        es.indices.update_aliases(body)
        connectionsDsl.remove_connection(index_config['connection_name'])
