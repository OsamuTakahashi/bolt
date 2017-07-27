package com.sopranoworks.bolt

import org.specs2.mutable.Specification

class LikeMatcherTest extends Specification {

  "apply" should {
    "test%" in {
      LikeMatcher("test%").findFirstIn("testok").isDefined must_== true
      LikeMatcher("test%").findFirstIn("test").isDefined must_== false
      LikeMatcher("test%").findFirstIn(" testok").isDefined must_== false
    }
    "test$%" in {
      LikeMatcher("test$%").findFirstIn("test$ok").isDefined must_== true
      LikeMatcher("test$%").findFirstIn("test$").isDefined must_== false
      LikeMatcher("test$%").findFirstIn(" test$ok").isDefined must_== false
    }
    "test\\%%" in {
      LikeMatcher("test\\%%").findFirstIn("test%ok").isDefined must_== true
      LikeMatcher("test\\%%").findFirstIn("test%").isDefined must_== false
      LikeMatcher("test\\%%").findFirstIn(" test%ok").isDefined must_== false
    }
    "test_" in {
      LikeMatcher("test_").findFirstIn("test!").isDefined must_== true
      LikeMatcher("test_").findFirstIn("test").isDefined must_== false
      LikeMatcher("test_").findFirstIn(" test!").isDefined must_== false
    }
    "test[%]" in {
      LikeMatcher("test[%]").findFirstIn("test[test]").isDefined must_== true
      LikeMatcher("test[%]").findFirstIn("test[]").isDefined must_== false
      LikeMatcher("test[%]").findFirstIn(" test[test]").isDefined must_== false
    }
  }
}
