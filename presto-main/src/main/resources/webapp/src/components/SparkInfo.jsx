import React from "react";

export class SparkInfo extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        return (
        <div>
            <div className="panel panel-primary">
                <div className="panel-heading">Spark Query Input Box</div>
                <div className="panel-body">
                    Query Input Box. TBD.
                </div>
                <div className="panel-footer">
                    Show code and result here.
                </div>
            </div>

            <div className="panel panel-success">
                <div className="panel-heading">Query Result</div>
                <div className="panel-body">
                    Execution time & summary for result.
                </div>
            </div>
        </div>);
    }
}