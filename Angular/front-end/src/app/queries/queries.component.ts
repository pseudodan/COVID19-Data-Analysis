/* *** Queries Component ***
*  Holds the selected query that the user has chosen.
*  Used in app.component.html
*   - selectedQuery: Type Query from queries.model.ts. That holds the query.
*/

import { Component, OnInit } from '@angular/core';
import { Query } from './queries.model';
@Component({
  selector: 'app-queries',
  templateUrl: './queries.component.html',
  styleUrls: ['./queries.component.css']
})
export class QueriesComponent implements OnInit {
  // Grab the selected query from queries.model
  // Queries-list has all the data
  selectedQuery: Query;
  constructor() { }

  ngOnInit(): void {
  }

}
