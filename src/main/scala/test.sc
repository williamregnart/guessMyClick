val pattern = """IAB\d{1,2}""".r
pattern.findAllIn("IAB17-4").mkString