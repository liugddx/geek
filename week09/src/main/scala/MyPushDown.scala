case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] {
  logWarning(msg = "MyPushDown Start")
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Multiply(left, right, failOnError) if right.isInstanceOf[Literal]
      && right.asInstanceOf[Literal].value.asInstanceOf[Int] == 1 =>
      left
    case Multiply(left, right, failOnError) if left.isInstanceOf[Literal]
      && left.asInstanceOf[Literal].value.asInstanceOf[Int] == 1 =>
      right
  }
}
