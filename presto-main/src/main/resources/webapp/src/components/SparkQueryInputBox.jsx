/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React from "react";

import query_code_1 from "!!raw-loader!../SparkQuery/UniSelectQuery1.scala";
import query_code_2 from "!!raw-loader!../SparkQuery/UniSelectQuery2.scala";
import query_code_3 from "!!raw-loader!../SparkQuery/UniSelectQuery3.scala";

export class SparkQueryInputBox extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            query_id: '',
            errorMessage: ''
        };

        this.handleChangeSelect = this.handleChangeSelect.bind(this);
        this.executeQuery = this.executeQuery.bind(this);
    }

    static obj2Json(obj) {
        return JSON.stringify(obj);
    }

    static json2Obj(json) {
        return JSON.parse(json);
    }

    handleChangeSelect(event) {
        this.setState({
            query_id: event.target.value
        });
    }

    executeQuery(event) {
        const jar_name = "/home/kingbaseUAE/spark-3.1.2/SparkProject/target/SparkProject-1.0-SNAPSHOT.jar";
        const class_name = "cn.edu.ruc.luowenxu.neo4j." + this.state.query_id;

        let requestBody = {
            "appResource": jar_name,
            "clientSparkVersion": "3.1.2",
            "mainClass": class_name,
            "action": "CreateSubmissionRequest",
            "environmentVariables": {
                "SPARK_ENV_LOADED": "1"
            },
            "sparkProperties": {
                "spark.master": "spark://worker204:8002",
                "spark.driver.memory": "5g",
                "spark.driver.cores": "1",
                "spark.executor.memory": "30g",
                "spark.executor.cores": "4",
                "spark.executor.instances": "2",
                "spark.app.name": this.state.query_id,
                "spark.driver.supervise": "true",
                "spark.jars": jar_name
            },
            "appArgs": []
        };
        let queryRequest = {
            method: 'POST',
            body: SparkQueryInputBox.obj2Json(requestBody)
        };

        fetch('http://10.77.50.204:6066/v1/submissions/create/', queryRequest)
            .then(function(response) {
                if (!response.ok) {
                    console.log("Error")
                    return ""
                }
                return response.json()
            })
            .then(function(json) {
                console.log(json)
            });

        this.setState({
            query_id: '',
            errorMessage: ''
        });

        let query_code;
        switch (this.state.query_id) {
            case "UniSelectQuery1":
                query_code = query_code_1;
                break;
            case "UniSelectQuery2":
                query_code = query_code_2;
                break;
            case "UniSelectQuery3":
                query_code = query_code_3;
                break;
            default:
                query_code = "Unknown";
        }

        this.props.father_node.setState({
            query_code: query_code
        });

        event.preventDefault();
    }

    render() {
        let spark_queries = [ "UniSelectQuery1", "UniSelectQuery2", "UniSelectQuery3"];
        spark_queries = spark_queries.map(function (str, idx) {
            return (
                <option value={str} key={"Spark_" + (idx+1)}>Query #{idx+1}: {str}</option>
            );
        });

        return (
            <div className="row">
                <div className="toolbar-col">
                    <div className="input-group input-group-sm">
                        <input placeholder="Select a Spark query." className="form-control form-control-small search-bar"
                               list="some_spark_queries" onChange={this.handleChangeSelect} value={this.state.query_id} />
                        <datalist id="some_spark_queries">
                            {spark_queries}
                        </datalist>

                        <div className="input-group-btn">
                            <button type="button" className="btn btn-default" onClick={this.executeQuery}>Submit</button>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}
