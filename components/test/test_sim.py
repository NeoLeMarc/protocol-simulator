import unittest
from unittest.mock import Mock, MagicMock
from components.sim import *
from infrastructure.aws import *

class TestSim(unittest.TestCase):

    def test_setup(self):
        r1 = Mock(AWSRegion)
        env = Mock()
        sim1 = Sim(env, r1, 0) 

        r1.registerService.assert_called_with(sim1, 'SIM_0')

    def test_message(self):
        r1 = Mock(AWSRegion)
        env = Mock()
        sim1 = Sim(env, r1, 0)
        sim1.receive = Mock()
        sim1.receive("Testmessage")
