package edu.asu.zoophy.genbankfactory.database;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Author {

	private String firstName;
	private String initial;
	private String lastName;
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getInitial() {
		return initial;
	}
	public void setInitial(String initial) {
		this.initial = initial;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	@Override
	public String toString() {
		return "Author [firstName=" + firstName + ", initial=" + initial + ", lastName=" + lastName + "]";
	}
	
	 @Override
	 public int hashCode() {
	        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
	            // if deriving: appendSuper(super.hashCode()).
	            append(firstName).
	            append(initial).
	            append(lastName).
	            toHashCode();
	    }
	 @Override
	 public boolean equals(Object obj) {
	       if (!(obj instanceof Author))
	            return false;
	        if (obj == this)
	            return true;

	        Author target = (Author) obj;
	        return new EqualsBuilder().
	            // if deriving: appendSuper(super.equals(obj)).
	            append(firstName, target.firstName).
	            append(initial, target.initial).
	            append(lastName, target.lastName).
	            isEquals();
	    }
	
}
