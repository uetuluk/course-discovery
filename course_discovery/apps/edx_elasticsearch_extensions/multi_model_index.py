from itertools import chain

from .abstract_index import DocumentBase


class MultiModelIndexBase(DocumentBase):

    index_model_mappings = []

    def get_index_models(self):
        return [x[1] for x in self.index_model_mappings]

    def get_index_queryset(self):
        qsets = []
        for index_class in self.get_index_models():
            qsets.append(index_class().get_index_queryset_for_model())
        return list(chain(*qsets))

    def get_updated_field(self):
        return None

    def create_document_dict(self, obj):
        return {}
