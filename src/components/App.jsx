import React from 'react';
import Header from './header/Header';
import Footer from './Footer';
import { loadSearchFromUrl } from '../actions/search';

export default class AppWrapper extends React.Component {
  constructor(props) {
    super(props)
  }

  // bootstrap search state in url (if any) when app mounts
  componentDidMount() {
    loadSearchFromUrl()
  }

  render() {
    return (
      <div className='app-container'>
        <div className='page-wrap'>
          <Header />
          {this.props.children}
        </div>
        <Footer />
      </div>
    )
  }
}