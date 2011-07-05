from AsyncStageOut.TransferWrapper import TransferWrapper
import random

class FakeTransfers(TransferWrapper):
    def command(self, files, userProxy, destSites):
        """
        In this transfer wrapper a random population is taken as being successfully transferred
        incomplete and the rest marked as a failure.
        """
        transferred = random.sample(files, random.randint(0, len(files)))
        for f in transferred:
            files.remove(f)
        incomplete = random.sample(files, random.randint(0, len(files)))
        for f in incomplete:
            files.remove(f)

        return files, transferred, incomplete
