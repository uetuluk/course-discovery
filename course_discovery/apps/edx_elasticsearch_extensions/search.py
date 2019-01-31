from django.conf import settings

from elasticsearch_dsl.connections import connections as connectionsDsl

from course_discovery.apps.course_metadata.search_indexes_dsl import CourseIndexDsl


class ElasticSearchQuery:

    def get_index_config(self, index_name):
        """
        Returns the index config on which current actions are performed.
        """
        connections = settings.ES_CONNECTIONS
        indexes = settings.ES_INDEXES

        for name, conn in connections.items():
            # gets name and connection value
            # default, {'hosts': 'localhost'}
            for index, value in indexes.get(name):
                # get indexes in that connection by name
                if index == index_name:
                    # return the desired index
                    return {
                        'connection_name': name,
                        'connection': conn,
                        'index_name': index,
                        'index_classes': value,
                    }
        return None

    def search(self, index_name, content_type, query):
        index_config = self.get_index_config(index_name)
        connectionsDsl.create_connection(
            hosts=index_config['connection']['hosts']
        )
        results = CourseIndexDsl.search().query('match', content_type=content_type).query('match', _all='{}'.format(query)).execute()
        connectionsDsl.remove_connection(index_config['connection_name'])
        return results
