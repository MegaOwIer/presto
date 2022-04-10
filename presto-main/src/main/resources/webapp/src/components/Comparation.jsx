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

export class Comp extends React.Component {
    constructor(props) {
        super(props);

        this.updateFigure = this.updateFigure.bind(this)
        this.resetFigure = this.resetFigure.bind(this)
        this.recallFigure = this.recallFigure.bind(this)

        this.option = {
            tooltip : {
                trigger: 'item',
                formatter: "{a} <br/>{b} : {c} ms"
            },
            grid: { top: 30, right: 10, bottom: 30, left: 45 },
            legend: {
                left: 'center',
                textStyle: {
                    color: '#FFFFFF',
                    fontSize: '16px'
                },
                data: ['Gourd Store', 'Spark']
            },
            calculable : true,
            xAxis : [{ type : 'category', data : [] }],
            yAxis : [
                {
                    type: 'value',
                    name: "",
                    nameLocation: "center",
                    nameGap: 35,
                    nameRotate: 0,
                    nameTextStyle: {
                        fontSize: 16,
                    },
                    axisLabel : {
                        show: true,
                        showMinLabel: true,
                        showMaxLabel: true,
                        formatter: function (value) {
                            return value;
                        }
                    }
                }
            ],
            series : [
                {
                    name: 'Gourd Store',
                    type: 'bar',
                    yAxisIndex: 0,
                    data: []
                },
                {
                    name: 'Spark',
                    type: 'bar',
                    yAxisIndex: 0,
                    data: []
                }
            ]
        };
    }

    resetFigure() {
        this.option.xAxis[0].data = [];
        this.option.series[0].data = [];
        this.option.series[1].data = [];

        let myChart = echarts.init(document.getElementById('cmp_figure'));
        myChart.setOption(this.option);
    }

    updateFigure() {
        this.option.xAxis[0].data.push('#' + (this.option.xAxis[0].data.length + 1));
        this.option.series[0].data.push(this.props.father_node.state.presto_query_time);
        this.option.series[1].data.push(this.props.father_node.state.spark_query_time);

        let myChart = echarts.init(document.getElementById('cmp_figure'));
        myChart.setOption(this.option);
    }

    recallFigure() {
        if(this.option.xAxis[0].data.length === 0) {
            return;
        }

        this.option.xAxis[0].data.pop();
        this.option.series[0].data.pop();
        this.option.series[1].data.pop();

        let myChart = echarts.init(document.getElementById('cmp_figure'));
        myChart.setOption(this.option);
    }

    render() {
        return (
            <div>
                <div className="panel panel-default" style={{'borderColor': 'transparent'}}>
                    <div>
                        <div id="cmp_figure" style={{'width': '500px', 'height': '350px'}}></div>
                    </div>
                    <div className="input-group-btn" align="center">
                        <button type="button" className="btn btn-default" onClick={this.resetFigure}>Reset</button>
                        &nbps;&nbps;
                        <button type="button" className="btn btn-default" onClick={this.updateFigure}>Update</button>
                        &nbps;&nbps;
                        <button type="button" className="btn btn-default" onClick={this.recallFigure}>Remove Last</button>
                    </div>
                </div>
            </div>
        );
    }
}