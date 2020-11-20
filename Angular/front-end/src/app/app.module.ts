/* *** App Module ***
*  Every newly created component must be added here
*/
import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { HeaderComponent } from './header/header.component';
import { QueriesComponent } from './queries/queries.component';
import { QueriesListComponent } from './queries/queries-list/queries-list.component';
import { QueriesListItemComponent } from './queries/queries-list/queries-list-item/queries-list-item.component';
import { QueriesDetailComponent } from './queries/queries-detail/queries-detail.component';

@NgModule({
  declarations: [
    AppComponent,
    HeaderComponent,
    QueriesComponent,
    QueriesListComponent,
    QueriesListItemComponent,
    QueriesDetailComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
