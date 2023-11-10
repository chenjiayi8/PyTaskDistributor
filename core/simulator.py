""" This module contains the abstract class for a simulation task. """

from abc import ABC, abstractmethod


class Simulator(ABC):
    """This class is an abstract class for a simulation task."""

    def __init__(self, setup) -> None:
        self.hostname = setup["hostname"]
        self.role = setup["role"]
        self.home = setup["home"]

    @abstractmethod
    def generate(self) -> list[str]:
        """Method to generate the simulation tasks."""

    @abstractmethod
    def dispatch(self, servers) -> list[str]:
        """Method to dispatch the simulation tasks."""

    @abstractmethod
    def run(self):
        """Method to run the simulation."""

    @abstractmethod
    def monitor(self):
        """Method to monitor the simulation."""

    @abstractmethod
    def finalise(self):
        """Method to finalise the simulation."""

    @abstractmethod
    def summarise(self) -> dict:
        """Method to summarise the simulation."""
