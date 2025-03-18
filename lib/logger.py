class Log4j:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger("sbdl")

    def warn(self, msg):
        return self.logger.warn(msg)

    def info(self, msg):
        return self.logger.info(msg)

    def error(self, msg):
        return self.logger.error(msg)

    def debug(self, msg):
        return self.logger.debug(msg)