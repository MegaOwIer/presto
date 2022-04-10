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

import query_code_1 from "!!raw-loader!../SampleQuery/Spark/UniSelectQuery1.scala";
import query_code_2 from "!!raw-loader!../SampleQuery/Spark/UniSelectQuery2.scala";
import query_code_3 from "!!raw-loader!../SampleQuery/Spark/UniSelectQuery3.scala";
import query_code_4 from "!!raw-loader!../SampleQuery/Spark/UniSelectQuery4.scala";
import query_code_5 from "!!raw-loader!../SampleQuery/Spark/UniSelectQuery5.scala";
import query_code_6 from "!!raw-loader!../SampleQuery/Spark/UniSelectQuery6.scala";
import query_code_7 from "!!raw-loader!../SampleQuery/Spark/UniSelectQuery7.scala";
import query_code_8 from "!!raw-loader!../SampleQuery/Spark/UniSelectQuery8.scala";
import query_code_9 from "!!raw-loader!../SampleQuery/Spark/UniSelectQuery9.scala";
import query_code_10 from "!!raw-loader!../SampleQuery/Spark/UniSelectQuery10.scala";


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
                "spark.master": "spark://10.77.50.204:7077",
                "spark.driver.memory": "5g",
                "spark.driver.cores": "1",
                "spark.executor.memory": "45g",
                "spark.executor.cores": "6",
                "spark.executor.instances": "6",
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

        const query_codes = [
            query_code_1, query_code_2, query_code_3, query_code_4, query_code_5,
            query_code_6, query_code_7, query_code_8, query_code_9, query_code_10
        ];
        let query_code = query_codes[this.state.query_id.slice(14) - 1];

        this.props.father_node.setState({
            query_code: query_code
        });

        event.preventDefault();
    }

    render() {
        let spark_queries = [ ];
        for (let id = 1; id <= 10; id += 1) {
            spark_queries.push("UniSelectQuery" + id);
        }
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
