'use strict'

const e = React.createElement

class Subgraphs extends React.Component {
  constructor(props) {
    super(props)
    this.state = { liked: false }
  }

  subgraphs() {
    return {
      Memefactory: '123',
      Decentraland: '234',
      ENS: '123',
      Bitsausage: '123'
    }
  }

  render() {
    const subgraphs = Object.keys(this.subgraphs())
    return (
      <div className="root">
        <span className="shape1" />
        <span className="shape2" />
        <span className="shape3" />
        <span className="shape4" />
        <section className="top">
          <div className="title">Subgraphs</div>
          <div className="subtitle">
            Check out the newest and the coolest subgraphs!
          </div>
        </section>
        <section className="subgraphs">
          {subgraphs.map((subgraph, index) => (
            <div className="subgraph" key={index}>
              <div className="header">
                <div className="name">{subgraph}</div>
                <span className="divider" />
              </div>
              <div className="content">
                <div className="text-group">
                  <div className="text">
                    <div className="type">Playground</div>
                    <div className="link">{`http://blah.com/${subgraph.toLowerCase()}`}</div>
                  </div>
                  <a
                    href={`http://${subgraph.toLowerCase()}.com/subgraph`}
                    target="_blank"
                  >
                    <img className="icon" src="./images/link.svg" />
                  </a>
                </div>
                <div className="text-group">
                  <div className="text">
                    <div className="type">Queries (HTTP)</div>
                    <div className="link">
                      {`http://blah.com/${subgraph.toLowerCase()}/graphiql`}
                    </div>
                  </div>
                  <a
                    href={`http://blah.com/${subgraph.toLowerCase()}/graphiql`}
                    target="_blank"
                  >
                    <img className="icon" src="./images/link.svg" />
                  </a>
                </div>
                <div className="text-group">
                  <div className="text">
                    <div className="type">Subscriptions (WS)</div>
                    <div className="link">{`ws://${subgraph.toLowerCase()}.com/subgraph`}</div>
                  </div>
                  <a
                    href={`ws://${subgraph.toLowerCase()}.com/subgraph`}
                    target="_blank"
                  >
                    <img className="icon" src="./images/link.svg" />
                  </a>
                </div>
              </div>
            </div>
          ))}
        </section>
        <div className="theg" />
      </div>
    )
  }
}

ReactDOM.render(e(Subgraphs, {}), document.getElementById('subgraphs'))
