# Citations

If you use BigAsterisk in academic work, cite the paper for the specific technique you
used. If you refer to the platform as a whole, cite the tools you actually exercised
rather than the repository.

## Debugging

```bibtex
@article{titian-pvldb16,
  author  = {Interlandi, Matteo and Shah, Kshitij and Tetali, Sai Deep and
             Gulzar, Muhammad Ali and Yoo, Seunghyun and Kim, Miryung and
             Millstein, Todd and Condie, Tyson},
  title   = {Titian: Data Provenance Support in Spark},
  journal = {Proceedings of the VLDB Endowment},
  volume  = {9},
  number  = {3},
  pages   = {216--227},
  year    = {2015}
}

@inproceedings{bigdebug-icse16,
  author    = {Gulzar, Muhammad Ali and Interlandi, Matteo and Yoo, Seunghyun and
               Tetali, Sai Deep and Condie, Tyson and Millstein, Todd and Kim, Miryung},
  title     = {BigDebug: Debugging Primitives for Interactive Big Data Processing in Spark},
  booktitle = {Proceedings of the 38th International Conference on Software Engineering
               (ICSE)},
  year      = {2016},
  doi       = {10.1145/2884781.2884813}
}

@inproceedings{vega-socc16,
  author    = {Interlandi, Matteo and Tetali, Sai Deep and Gulzar, Muhammad Ali and
               Noor, Joseph and Condie, Tyson and Kim, Miryung and Millstein, Todd},
  title     = {Optimizing Interactive Development of Data-Intensive Applications},
  booktitle = {Proceedings of the Seventh ACM Symposium on Cloud Computing (SoCC)},
  year      = {2016},
  doi       = {10.1145/2987550.2987565}
}

@inproceedings{bigsift-socc17,
  author    = {Gulzar, Muhammad Ali and Interlandi, Matteo and Han, Xueyuan and
               Li, Mingda and Condie, Tyson and Kim, Miryung},
  title     = {Automated Debugging in Data-Intensive Scalable Computing},
  booktitle = {Proceedings of the 2017 Symposium on Cloud Computing (SoCC)},
  year      = {2017},
  doi       = {10.1145/3127479.3131624}
}

@inproceedings{perfdebug-socc19,
  author    = {Teoh, Jason and Gulzar, Muhammad Ali and Xu, Guoqing Harry and
               Kim, Miryung},
  title     = {PerfDebug: Performance Debugging of Computation Skew in Dataflow Systems},
  booktitle = {Proceedings of the 2019 Symposium on Cloud Computing (SoCC)},
  year      = {2019},
  doi       = {10.1145/3357223.3362727}
}

@inproceedings{flowdebug-socc20,
  author    = {Teoh, Jason and Gulzar, Muhammad Ali and Kim, Miryung},
  title     = {Influence-Based Provenance for Dataflow Applications with Taint Propagation},
  booktitle = {Proceedings of the 11th ACM Symposium on Cloud Computing (SoCC)},
  year      = {2020},
  doi       = {10.1145/3419111.3421292}
}

@inproceedings{optdebug-socc21,
  author    = {Gulzar, Muhammad Ali and Kim, Miryung},
  title     = {OptDebug: Fault-Inducing Operation Isolation for Dataflow Applications},
  booktitle = {Proceedings of the 12th ACM Symposium on Cloud Computing (SoCC)},
  year      = {2021},
  doi       = {10.1145/3472883.3487021}
}

@inproceedings{desql-fse24,
  author    = {Haroon, Sabaat and Brown, Chris and Gulzar, Muhammad Ali},
  title     = {DeSQL: Interactive Debugging of SQL in Data-Intensive Scalable Computing},
  booktitle = {Proceedings of the ACM International Conference on the Foundations of
               Software Engineering (FSE)},
  year      = {2024},
  doi       = {10.1145/3643761}
}
```

## Testing

```bibtex
@inproceedings{bigtest-fse19,
  author    = {Gulzar, Muhammad Ali and Mardani, Shaghayegh and Musuvathi, Madanlal and
               Kim, Miryung},
  title     = {White-Box Testing of Big Data Analytics with Complex User-Defined Functions},
  booktitle = {Proceedings of the 27th ACM Joint European Software Engineering Conference
               and Symposium on the Foundations of Software Engineering (ESEC/FSE)},
  year      = {2019},
  doi       = {10.1145/3338906.3338953}
}

@inproceedings{bigtest-icse20,
  author    = {Gulzar, Muhammad Ali and Musuvathi, Madanlal and Kim, Miryung},
  title     = {BigTest: A Symbolic Execution Based Systematic Test Generation Tool for
               Apache Spark},
  booktitle = {Proceedings of the 42nd International Conference on Software Engineering
               (ICSE Companion)},
  year      = {2020},
  doi       = {10.1145/3377812.3382145}
}

@inproceedings{bigfuzz-ase20,
  author    = {Zhang, Qian and Wang, Jiyuan and Gulzar, Muhammad Ali and
               Padhye, Rohan and Kim, Miryung},
  title     = {BigFuzz: Efficient Fuzz Testing for Data Analytics Using Framework
               Abstraction},
  booktitle = {Proceedings of the 35th IEEE/ACM International Conference on Automated
               Software Engineering (ASE)},
  year      = {2020},
  doi       = {10.1145/3324884.3416641}
}

@inproceedings{depfuzz-fse23,
  author    = {Humayun, Ahmad and Kim, Miryung and Gulzar, Muhammad Ali},
  title     = {Co-dependence Aware Fuzzing for Dataflow-Based Big Data Analytics},
  booktitle = {Proceedings of the 31st ACM Joint European Software Engineering Conference
               and Symposium on the Foundations of Software Engineering (ESEC/FSE)},
  year      = {2023},
  doi       = {10.1145/3611643.3616298}
}

@inproceedings{naturalfuzz-ase23,
  author    = {Humayun, Ahmad and Wu, Yaoxuan and Kim, Miryung and
               Gulzar, Muhammad Ali},
  title     = {NaturalFuzz: Natural Input Generation for Big Data Analytics},
  booktitle = {Proceedings of the 38th IEEE/ACM International Conference on Automated
               Software Engineering (ASE)},
  year      = {2023}
}

@inproceedings{naturalsym-fse24,
  author    = {Wu, Yaoxuan and Humayun, Ahmad and Gulzar, Muhammad Ali and Kim, Miryung},
  title     = {Natural Symbolic Execution-based Testing for Big Data Analytics},
  booktitle = {Proceedings of the ACM International Conference on the Foundations of
               Software Engineering (FSE)},
  year      = {2024}
}
```

## Related

```bibtex
@inproceedings{bigdebug-demo-fse16,
  author    = {Gulzar, Muhammad Ali and Interlandi, Matteo and Condie, Tyson and
               Kim, Miryung},
  title     = {BigDebug: Interactive Debugger for Big Data Analytics in Apache Spark},
  booktitle = {Proceedings of the 24th ACM SIGSOFT International Symposium on the
               Foundations of Software Engineering (FSE), Demonstrations},
  year      = {2016},
  doi       = {10.1145/2950290.2983930}
}

@inproceedings{bigsift-demo-fse18,
  author    = {Gulzar, Muhammad Ali and Wang, Siman and Kim, Miryung},
  title     = {BigSift: Automated Debugging of Big Data Analytics in Data-Intensive
               Scalable Computing},
  booktitle = {Proceedings of the 26th ACM Joint European Software Engineering Conference
               and Symposium on the Foundations of Software Engineering (ESEC/FSE),
               Demonstrations},
  year      = {2018},
  doi       = {10.1145/3236024.3264586}
}

@inproceedings{bigdatadebugging-hotcloud16,
  author    = {Gulzar, Muhammad Ali and Han, Xueyuan and Interlandi, Matteo and
               Mardani, Shaghayegh and Tetali, Sai Deep and Condie, Tyson and
               Millstein, Todd and Kim, Miryung},
  title     = {Interactive Debugging for Big Data Analytics},
  booktitle = {8th USENIX Workshop on Hot Topics in Cloud Computing (HotCloud)},
  year      = {2016}
}
```
