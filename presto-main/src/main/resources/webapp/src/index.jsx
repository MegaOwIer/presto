import React from "react";
import ReactDOM from "react-dom";
import {PageTitle} from "./components/PageTitle";
import {PrestoInfo} from "./components/PrestoInfo";
import {SparkInfo} from "./components/SparkInfo";
import {Comp} from "./components/Comparation";

ReactDOM.render(
    <PageTitle title="Gourd Store" />,
    document.getElementById('title')
);

class Main extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            presto_query_time: 0,
            spark_query_time: 0
        };
    }

    render() {
        return (
            <div>
                <div id="presto-info" className="col-sm-12 col-md-7">
                    <PrestoInfo father_node={this} />
                </div>

                <div id="compare" className="col-sm-12 col-md-5">
                    <Comp father_node={this} />
                </div>

                <div id="spark-info" className="col-sm-12 col-md-5">
                    <SparkInfo father_node={this} />
                </div>
            </div>
        );
    }
}

ReactDOM.render(
    <Main />,
    document.getElementById("main")
)