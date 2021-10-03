resource "aws_sfn_state_machine" "step_func_state_mach" {
    name = "${var.step_name}"
    role_arn = "${var.step_role_arn}"

  definition = <<DEFINITION
{
  "StartAt": "API pipeline",
  "States": {
    "API pipeline": {
      "Type": "Task",
      "Resource": "arn:aws:states:::ecs:runTask.waitForTaskToken",
      "Parameters": {
        "LaunchType": "FARGATE",
        "Cluster": "arn:aws:ecs:eu-west-1:xxx:cluster/xxx",
        "TaskDefinition": "arn:aws:ecs:eu-west-1:xxx:task-definition/${var.container_name}:1",
        "NetworkConfiguration": {
          "AwsvpcConfiguration": {
            "Subnets": [
              "subnet-xxx",
              "subnet-xxx"
            ],
            "SecurityGroups": [
              "sg-xxx"
            ],
            "AssignPublicIp": "DISABLED"
          }
        },
        "Overrides": {
          "ContainerOverrides": [
            {
              "Name": "${var.container_name}",
              "Environment": [
                {
                  "Name": "TASK_TOKEN",
                  "Value.$": "$$.Task.Token"
                },
                {
                  "Name": "EVENT_TO_PROCESS",
                  "Value.$": "States.JsonToString($)"
                }
              ]
            }
          ]
        }
      },
      "Next": "Lambda (Notification OK)",
      "Retry": [
        {
          "ErrorEquals": [
            "-1"
          ],
          "IntervalSeconds": 10,
          "BackoffRate": 2,
          "MaxAttempts": 3
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "Send Email"
        }
      ]
    },
    "END_SUCCESS": {
      "Type": "Succeed"
    },
    "Lambda (Notification OK)": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:eu-west-1:xxx:function:pipelines_notification:$LATEST",
        "Payload": {
          "msg.$": "$"
        }
      },
      "Next": "END_SUCCESS"
    },
    "Lambda (Notification Failure)": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:eu-west-1:xxx:function:pipelines_notification:$LATEST",
        "Payload": {
          "msg": "${var.api_name}  pipeline has failed, details have been sent by email"
        }
      },
      "Next": "Fail State"
    },
    "Send Email": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "Message": {
          "Message.$": "$.Cause"
        },
        "TopicArn": "arn:aws:sns:eu-west-1:xxx:phd-data-pipelines-failure"
      },
      "Next": "Lambda (Notification Failure)"
    },
    "Fail State": {
      "Type": "Fail"
    }
  }
}
DEFINITION
}
