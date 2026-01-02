from abc import ABC


class Object(ABC):
    """
    Base class for all objects.
    """

    def __init__(self, name: str):
        self.name = name

    def on_add_to_pipeline(self):
        """
        This function is called once the object is added to the pipeline.
        """
        pass

    def on_remove_from_pipeline(self):
        """
        This function is called once the object is removed from the pipeline.
        """
        pass

    def on_pipeline_start(self):
        """
        This function is called once the pipeline execution starts.
        """
        pass

    def on_pipeline_end(self):
        """
        This function is called once the pipeline execution ends.
        """
        pass

    def save(self):
        """
        This function is called once one iteration of the pipeline is completed.
        Therefore, if there are unsaved changes, they have to be saved permanently.
        """
        pass

    def purge(self):
        """
        This function is called when the contents of the object need to be reset
        to the initial state, i.e. the same state as after calling on_add_to_pipeline.

        An example use case is when the producer of the object is changed and needs to
        be re-executed.
        """
        pass

