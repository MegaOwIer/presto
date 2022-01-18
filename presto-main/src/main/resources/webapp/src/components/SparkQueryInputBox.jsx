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

import {
    truncateString
} from "../utils";

export class SparkQueryInputBox extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            jar_name: '',
            class_name: '',
            app_args: '',
            spark_properties: {
                "spark.master": "spark://172.17.197.143:7077",
                "spark.driver.memory": "1g",
                "spark.driver.cores": "1",
                "spark.worker.memory": "1g",
                "spark.worker.cores": "1",
                "spark.driver.supervise": "true"
            },
            errorMessage: ''
        };

        this.handleChangeJarName = this.handleChangeJarName.bind(this);
        this.handleChangeClassName = this.handleChangeClassName.bind(this);
        this.handleChangeArgs = this.handleChangeArgs.bind(this);
        this.handleChangeProps = this.handleChangeProps.bind(this);
        this.executeQuery = this.executeQuery.bind(this);
    }

    static obj2Json(obj) {
        return JSON.stringify(obj);
    }

    static json2Obj(json) {
        return JSON.parse(json);
    }

    handleChangeJarName(event) {
        this.setState({
            jar_name: event.target.value
        });
    }

    handleChangeClassName(event) {
        this.setState({
            class_name: event.target.value
        });
    }

    handleChangeArgs(event) {
        this.setState({
            app_args: event.target.value
        });
    }

    handleChangeProps(event) {
        this.setState({
            spark_properties: SparkQueryInputBox.json2Obj(event.target.value)
        });
    }

    executeQuery(event) {
        this.state.spark_properties["spark.jars"] = this.state.jar_name;

        let requestBody = {
            "appResource": this.state.jar_name,
            "clientSparkVersion": "3.1.2",
            "mainClass": this.state.class_name,
            "action": "CreateSubmissionRequest",
            "environmentVariables": {
                "SPARK_ENV_LOADED": "1"
            },
            "sparkProperties": this.state.spark_properties,
            "appArgs": [this.state.app_args]
        };
        let queryRequest = {
            method: 'POST',
            headers: {
                "Content-Type": "application/json;charset=UTF-8"
            },
            body: SparkQueryInputBox.obj2Json(requestBody)
        };

        let driverID = fetch('http://112.126.79.236:6066/v1/submissions/create/', queryRequest)
            .then(function(response) {
                if (!response.ok) {
                    console.log("Error")
                    return ""
                }
                return response.json()
            })
            .then(function(json) {
                return json["submissionId"];
            });

        this.setState({
            jar_name: '',
            class_name: '',
            app_args: '',
            spark_properties: {
                "spark.master": "spark://172.17.197.143:7077",
                "spark.driver.memory": "1g",
                "spark.driver.cores": "1",
                "spark.worker.memory": "1g",
                "spark.worker.cores": "1",
                "spark.driver.supervise": "true"
            },
            errorMessage: ''
        });
        this.props.father_node.setState({
            driverID: driverID
        });
        event.preventDefault();
    }

    render() {
        return (
            <div className="row">
                <div className="toolbar-col">
                    <div className="input-group input-group-sm">
                        <div>
                            <label>Target jars: </label>
                            <input type="text" className="form-control form-control-small search-bar"
                                   placeholder="Choose a .jar file." onChange={this.handleChangeJarName} value={this.state.jar_name} />
                        </div>

                        <div>
                            <label>Main Class Name: </label>
                            <input type="text" className="form-control form-control-small search-bar"
                                   placeholder="Main class name." onChange={this.handleChangeClassName} value={this.state.class_name} />
                        </div>

                        <div>
                            <label>Arguments: </label>
                            <input type="text" className="form-control form-control-small search-bar"
                                   placeholder="Args." onChange={this.handleChangeArgs} value={this.state.app_args} />
                        </div>

                        <div>
                            <label>Spark Properties: </label>
                            <textarea type="text" className="form-control form-control-small search-bar"
                                      style={{height: '120px', resize: "none"}}
                                      placeholder="Input your query here." onChange={this.handleChangeProps}
                                      value={SparkQueryInputBox.obj2Json(this.state.spark_properties)} />
                        </div>

                        <div className="input-group-btn">
                            <button type="button" className="btn btn-default" onClick={this.executeQuery}>Submit</button>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}
