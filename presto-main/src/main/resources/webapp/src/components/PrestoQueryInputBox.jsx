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

export class PrestoQueryInputBox extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            query_str: '',
            errorMessage: ''
        };

        this.handleChange = this.handleChange.bind(this);
        this.executeQuery = this.executeQuery.bind(this);
    }

    handleChange(event) {
        this.setState({
            query_str: event.target.value
        });
    }

    static stripQueryTextWhitespace(queryText) {
        const lines = queryText.split("\n");
        let minLeadingWhitespace = -1;
        for (let i = 0; i < lines.length; i++) {
            if (minLeadingWhitespace === 0) {
                break;
            }

            if (lines[i].trim().length === 0) {
                continue;
            }

            const leadingWhitespace = lines[i].search(/\S/);

            if (leadingWhitespace > -1 && ((leadingWhitespace < minLeadingWhitespace) || minLeadingWhitespace === -1)) {
                minLeadingWhitespace = leadingWhitespace;
            }
        }

        let formattedQueryText = "";

        for (let i = 0; i < lines.length; i++) {
            const trimmedLine = lines[i].substring(minLeadingWhitespace).replace(/\s+$/g, '');

            if (trimmedLine.length > 0) {
                formattedQueryText += trimmedLine;

                if (i < (lines.length - 1)) {
                    formattedQueryText += "\n";
                }
            }
        }

        return truncateString(formattedQueryText, 300);
    }

    executeQuery(event) {
        // trim ';' and any white spaces from end of query string
        const query_str = this.state.query_str.replace(/[;\s]*$/, '');
        this.props.father_node.setState({
            lastQueryStr: PrestoQueryInputBox.stripQueryTextWhitespace(query_str),
            result_columns: [],
            result_data: []
        });

        let firstRequestOptions = {
            method: 'POST',
            headers: {
                'X-Presto-User': 'Webui-tester',
                'X-Presto-Source': 'Gourd Store WebUI'
            },
            body: query_str
        };
        let followRequestOptions = {
            method: 'GET',
            headers: {
                'X-Presto-User': 'Webui-tester',
                'X-Presto-Source': 'Gourd Store WebUI'
            }
        }

        const MAX_LENGTH = 20;

        function followQuery(targetUrl, requestBody, object) {
            fetch(targetUrl, requestBody)
                .then(response => response.json())
                .then(function(json) {
                    if ('columns' in json && object.state.result_columns.length === 0) {
                        let result = [];
                        json.columns.forEach(function (data) {
                            result = result.concat(data.name);
                        });
                        object.setState({
                            result_columns: result
                        });
                    }
                    if ('data' in json && object.state.result_data.length < MAX_LENGTH) {
                        // print query result to webpage.
                        object.setState({
                            result_data: object.state.result_data.concat(json.data).slice(0, MAX_LENGTH)
                        });
                        // console.log(json.data);
                    }
                    if ('nextUri' in json) {
                        followQuery(json.nextUri, followRequestOptions, object);
                    }
                })
                .catch(error => {
                    console.error('There was an error!', error.toString());
                    object.setState({
                        result_columns: [],
                        result_data: ["[Error] " + error.toString()]
                    });
                });
        }

        followQuery('/v1/statement', firstRequestOptions, this.props.father_node);
        this.setState({
            query_str: ''
        });
        event.preventDefault();
    }

    render() {
        return (
            <div className="row">
                <div className="toolbar-col">
                    <div className="input-group input-group-sm">
                        <textarea type="text" style={{resize: "none"}} className="form-control form-control-small search-bar"
                                  placeholder="Input your query here." onChange={this.handleChange} value={this.state.query_str} />
                        <div className="input-group-btn">
                            <button type="button" className="btn btn-default" onClick={this.executeQuery}>Submit</button>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}
